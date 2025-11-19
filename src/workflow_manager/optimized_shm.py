"""
优化的宿主机端共享内存管理器 - 基于描述符表的实现

核心优化：
1. 使用描述符表 + idle队列 + 哈希表
2. 宿主机只负责初始化，不参与运行时管理
3. 容器端负责描述符分配、查找、回收、数据读写
4. 完全消除Socket通信

内存布局：
[Header 256B] [Descriptor Table] [Bitmap] [Idle Queue] [Hash Table] [Data Area]

与容器端 optimized_container_shm.py 配合使用
"""

import struct
import time
import threading
import multiprocessing.shared_memory as shm
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

# 配置常量
SHM_SIZE = 5 * 1024 * 1024 * 1024  # 5GB
MAX_PACKETS = 5000  # 最大包数量
MAX_PACKET_SIZE = 1 * 1024 * 1024  # 1MB
SHM_MAGIC = 0x12345678

# 描述符状态
STATUS_FREE = 0
STATUS_ALLOCATED = 1
STATUS_WRITING = 2
STATUS_READY = 3
STATUS_READING = 4

# 标志位
FLAG_VALID = 1


@dataclass
class OptimizedPacketDescriptor:
    """
    优化的包描述符（56字节，简化版本）
    """
    data_offset: int = 0        # 8字节：数据在SHM中的偏移
    data_size: int = 0          # 4字节：数据大小
    function_id: int = 0        # 4字节：函数ID
    timestamp: int = 0          # 8字节：时间戳（微秒）
    flags: int = 0              # 4字节：标志位
    status: int = STATUS_FREE   # 4字节：状态
    dependency_count: int = 0   # 4字节：依赖计数
    reserved: bytes = b'\x00' * 20  # 20字节：预留，凑齐56字节
    
    def pack(self) -> bytes:
        """打包为56字节"""
        return struct.pack('<QIIQIII 20s',
                          self.data_offset,
                          self.data_size,
                          self.function_id,
                          self.timestamp,
                          self.flags,
                          self.status,
                          self.dependency_count,
                          self.reserved)
    
    @classmethod
    def unpack(cls, data: bytes) -> 'OptimizedPacketDescriptor':
        """从56字节解包"""
        if len(data) < 56:
            raise ValueError(f"描述符数据长度不足: {len(data)} < 56")
        values = struct.unpack('<QIIQIII 20s', data[:56])
        return cls(*values)
    
    @staticmethod
    def size() -> int:
        """返回描述符大小"""
        return 56


@dataclass
class OptimizedSharedMemoryHeader:
    """
    简化的共享内存头部结构（256字节，直接使用desc_index）
    """
    magic: int = SHM_MAGIC                    # 4字节：魔数
    version: int = 2                          # 4字节：版本号（v2表示优化版本）
    total_size: int = SHM_SIZE                # 8字节：总大小
    header_size: int = 124                    # 4字节：头部大小（恢复为256）
    descriptor_table_offset: int = 124        # 8字节：描述符表偏移
    descriptor_table_size: int = 0            # 8字节：描述符表大小
    bitmap_offset: int = 0                    # 8字节：位图偏移
    bitmap_size: int = 0                      # 8字节：位图大小
    idle_queue_offset: int = 0                # 8字节：空闲队列偏移
    idle_queue_size: int = 0                  # 8字节：空闲队列大小
    data_area_offset: int = 0                 # 8字节：数据区偏移
    data_area_size: int = 0                   # 8字节：数据区大小
    max_packets: int = MAX_PACKETS            # 4字节：最大包数量
    active_packets: int = 0                   # 4字节：活跃包数量
    idle_queue_head: int = 0                  # 4字节：空闲队列头指针
    idle_queue_tail: int = 0                  # 4字节：空闲队列尾指针
    
    reserved: bytes = b'\x00' * 24            # 24字节：预留（凑齐124字节）
    
    def pack(self) -> bytes:
        """将头部打包为124字节"""
        format_str = '<II Q I QQQQ QQQQ I I II 24s'
        print(f"[调试] 宿主机端格式字符串: {format_str}")
        
        # 计算格式字符串期望的字节数
        expected_size = struct.calcsize(format_str)
        print(f"[调试] 宿主机端格式字符串期望字节数: {expected_size}")
        
        packed_data = struct.pack(format_str,
                          self.magic,
                          self.version,
                          self.total_size,
                          self.header_size,
                          self.descriptor_table_offset,
                          self.descriptor_table_size,
                          self.bitmap_offset,
                          self.bitmap_size,
                          self.idle_queue_offset,
                          self.idle_queue_size,
                          self.data_area_offset,
                          self.data_area_size,
                          self.max_packets,
                          self.active_packets,
                          self.idle_queue_head,
                          self.idle_queue_tail,
                          self.reserved)
        
        print(f"[调试] 宿主机端实际打包字节数: {len(packed_data)}")
        print(f"[调试] 宿主机端前64字节: {packed_data[:64].hex()}")
        
        return packed_data
    
    @classmethod
    def unpack(cls, data: bytes) -> 'OptimizedSharedMemoryHeader':
        """从124字节解包头部"""
        if len(data) < 124:
            raise ValueError(f"头部数据长度不足: {len(data)} < 124")
        values = struct.unpack('<II Q I QQQQ QQQQ I I II 24s', data[:124])
        return cls(*values)
    
    @staticmethod
    def size() -> int:
        """返回头部大小"""
        return 124


class OptimizedHostSharedMemoryManager:
    """
    优化的宿主机共享内存管理器
    
    职责：
    - 初始化共享内存池（Header、描述符表、位图、idle队列、哈希表、数据区）
    - 不参与运行时管理（分配、查找、回收由容器端完成）
    """
    
    def __init__(self, shm_name: str = 'faasflow_shm_opt', shm_size: int = SHM_SIZE):
        self.shm_name = shm_name
        self.shm_size = shm_size
        self.shared_memory = None
        self.header: Optional[OptimizedSharedMemoryHeader] = None
        self.lock = threading.Lock()
        self._initialized = False
        self._is_creator = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._monitor_stop = threading.Event()
        self._monitor_interval = 5.0
        
        # 计算内存布局
        self.header_size = OptimizedSharedMemoryHeader.size()  # 256
        self.descriptor_size = OptimizedPacketDescriptor.size()  # 64
        
        # 描述符表
        self.descriptor_table_offset = self.header_size
        self.descriptor_table_size = MAX_PACKETS * self.descriptor_size
        
        # 位图
        self.bitmap_offset = self.descriptor_table_offset + self.descriptor_table_size
        self.bitmap_size = (MAX_PACKETS + 7) // 8
        
        # 空闲队列（存储desc_index，每个4字节）
        self.idle_queue_offset = self.bitmap_offset + self.bitmap_size
        self.idle_queue_size = MAX_PACKETS * 4
        
        # 数据区（256字节对齐）
        self.data_area_offset = self.idle_queue_offset + self.idle_queue_size
        self.data_area_offset = ((self.data_area_offset + 255) // 256) * 256
        self.data_area_size = self.shm_size - self.data_area_offset
        
        logger.info(f"简化SHM内存布局计划: "
                   f"Header={self.header_size}B, "
                   f"DescTable={self.descriptor_table_size}B, "
                   f"Bitmap={self.bitmap_size}B, "
                   f"IdleQueue={self.idle_queue_size}B, "
                   f"DataArea={self.data_area_size}B")
    
    def init_shm_pool(self) -> bool:
        """
        初始化共享内存池
        
        宿主机职责：
        1. 创建共享内存
        2. 初始化Header、描述符表、位图、idle队列、哈希表
        3. 不再参与运行时管理
        
        Returns:
            是否初始化成功
        """
        with self.lock:
            if self._initialized:
                logger.info(f"共享内存池 '{self.shm_name}' 已经初始化")
                return True
            
            try:
                # 尝试连接到现有的共享内存
                try:
                    self.shared_memory = shm.SharedMemory(name=self.shm_name)
                    self._is_creator = False
                    logger.info(f"连接到现有共享内存池: {self.shm_name}")
                    
                    # 读取头部验证
                    header_data = bytes(self.shared_memory.buf[:self.header_size])
                    self.header = OptimizedSharedMemoryHeader.unpack(header_data)
                    
                    if self.header.magic != SHM_MAGIC or self.header.version != 2:
                        logger.error("共享内存魔数或版本不匹配")
                        self.shared_memory.close()
                        self.shared_memory = None
                        return False
                    
                except FileNotFoundError:
                    # 创建新的共享内存
                    self.shared_memory = shm.SharedMemory(
                        name=self.shm_name,
                        create=True,
                        size=self.shm_size
                    )
                    self._is_creator = True
                    logger.info(f"创建新的共享内存池: {self.shm_name}, 大小={self.shm_size}B")
                    
                    # 初始化所有结构
                    self._initialize_shm_structures()
                
                self._initialized = True
                logger.info(f"共享内存池 '{self.shm_name}' 初始化成功")
                self.start_busy_monitor()
                return True
                
            except Exception as e:
                logger.error(f"初始化共享内存池失败: {e}")
                # 清理文件锁
                try:
                    self._cleanup_lock_files()
                except:
                    pass
                if self.shared_memory and self._is_creator:
                    try:
                        self.shared_memory.unlink()
                    except:
                        pass
                if self.shared_memory:
                    self.shared_memory.close()
                self.shared_memory = None
                self._initialized = False
                return False
    
    def _initialize_shm_structures(self):
        """初始化共享内存的各个结构"""
        # 1. 初始化Header
        self.header = OptimizedSharedMemoryHeader(
            magic=SHM_MAGIC,
            version=2,
            total_size=self.shm_size,
            header_size=self.header_size,
            descriptor_table_offset=self.descriptor_table_offset,
            descriptor_table_size=self.descriptor_table_size,
            bitmap_offset=self.bitmap_offset,
            bitmap_size=self.bitmap_size,
            idle_queue_offset=self.idle_queue_offset,
            idle_queue_size=self.idle_queue_size,
            data_area_offset=self.data_area_offset,
            data_area_size=self.data_area_size,
            max_packets=MAX_PACKETS,
            active_packets=0,
            idle_queue_head=0,
            idle_queue_tail=MAX_PACKETS - 1  # 初始队列满
        )
        
        # 使用bytes()转换避免memoryview赋值错误
        header_bytes = self.header.pack()
        for i, byte_val in enumerate(header_bytes):
            self.shared_memory.buf[i] = byte_val
        logger.info(f"✓ Header初始化完成: offset=0, size={self.header_size}B")
        
        # 2. 初始化描述符表
        desc_start = self.descriptor_table_offset
        for i in range(MAX_PACKETS):
            desc = OptimizedPacketDescriptor(
                data_offset=self.data_area_offset + (i * MAX_PACKET_SIZE),
                data_size=0,
                function_id=0,
                timestamp=0,
                flags=0,
                status=STATUS_FREE,
                dependency_count=0
            )
            desc_bytes = desc.pack()
            offset = desc_start + (i * self.descriptor_size)
            for j, byte_val in enumerate(desc_bytes):
                self.shared_memory.buf[offset + j] = byte_val
        
        logger.info(f"✓ 描述符表初始化完成: offset={self.descriptor_table_offset}, "
                   f"size={self.descriptor_table_size}B, count={MAX_PACKETS}")
        
        # 3. 初始化位图（全部为0，表示空闲）
        bitmap_start = self.bitmap_offset
        for i in range(self.bitmap_size):
            self.shared_memory.buf[bitmap_start + i] = 0
        logger.info(f"✓ 位图初始化完成: offset={self.bitmap_offset}, size={self.bitmap_size}B")
        
        # 4. 初始化空闲队列（包含所有描述符索引0-99）
        queue_start = self.idle_queue_offset
        for i in range(MAX_PACKETS):
            queue_bytes = struct.pack('<I', i)
            offset = queue_start + (i * 4)
            for j, byte_val in enumerate(queue_bytes):
                self.shared_memory.buf[offset + j] = byte_val
        
        logger.info(f"✓ 空闲队列初始化完成: offset={self.idle_queue_offset}, "
                   f"size={self.idle_queue_size}B, 初始数量={MAX_PACKETS}")
        
        # 4.5. 创建文件锁文件
        self._create_lock_files()
        
        # 5. 数据区不需要初始化
        logger.info(f"✓ 数据区: offset={self.data_area_offset}, size={self.data_area_size}B")
        
        logger.info("=" * 80)
        logger.info("所有共享内存结构初始化完成")
        logger.info("=" * 80)
        logger.info("宿主机职责：仅负责初始化，不参与运行时管理")
        logger.info("容器职责：负责描述符分配、查找、回收、数据读写")
        logger.info("=" * 80)
    
    # ------------------------------------------------------------------
    # Busy descriptor monitor
    # ------------------------------------------------------------------

    def start_busy_monitor(self, interval: float = 5.0):
        """启动忙描述符监控线程"""
        if not self.shared_memory:
            logger.warning("共享内存未连接，无法启动忙描述符监控")
            return
        self._monitor_interval = interval
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.debug("忙描述符监控已在运行，跳过启动")
            return
        self._monitor_stop.clear()
        self._monitor_thread = threading.Thread(
            target=self._busy_monitor_loop,
            name="FaaSFlowBusyMonitor",
            daemon=True,
        )
        self._monitor_thread.start()
        logger.info(f"忙描述符监控已启动，间隔 {interval}s")

    def stop_busy_monitor(self):
        """停止忙描述符监控线程"""
        if not self._monitor_thread:
            return
        self._monitor_stop.set()
        self._monitor_thread.join(timeout=2.0)
        self._monitor_thread = None
        self._monitor_stop = threading.Event()
        logger.info("忙描述符监控已停止")

    def _busy_monitor_loop(self):
        while not self._monitor_stop.is_set():
            try:
                self._log_busy_descriptors()
            except Exception as exc:
                logger.exception(f"忙描述符监控异常: {exc}")
            if self._monitor_stop.wait(self._monitor_interval):
                break

    def _log_busy_descriptors(self):
        header = self._read_current_header()
        if not header:
            return

        idle_indices = self._collect_idle_indices(header)
        idle_set = {idx for idx in idle_indices if 0 <= idx < header.max_packets}

        total_packets = header.max_packets
        busy_indices = sorted(set(range(total_packets)) - idle_set)
        busy_count = len(busy_indices)

        container_usage = defaultdict(int)
        sample_details = []
        for idx in busy_indices:
            descriptor = self._read_descriptor(idx)
            if descriptor is None:
                continue
            container_id = descriptor.function_id
            container_usage[container_id] += 1
            if len(sample_details) < 10:
                sample_details.append((idx, container_id, descriptor.status))

        sample = busy_indices[:10]
        sample_suffix = "..." if len(busy_indices) > len(sample) else ""

        top_users = sorted(container_usage.items(), key=lambda x: x[1], reverse=True)[:5]
        usage_str = ", ".join(f"{cid}:{cnt}" for cid, cnt in top_users)
        sample_str = ", ".join(f"{idx}(cid={cid},status={status})" for idx, cid, status in sample_details)

        msg = (f"[BusyMonitor] busy={busy_count}, idle={len(idle_set)}, "
               f"head={header.idle_queue_head}, tail={header.idle_queue_tail}, "
               f"top_containers=[{usage_str}], sample=[{sample_str}{sample_suffix}]")
        print(msg)
        logger.debug(msg)

    def _read_current_header(self) -> Optional[OptimizedSharedMemoryHeader]:
        if not self.shared_memory:
            return None
        try:
            header_bytes = bytes(self.shared_memory.buf[:self.header_size])
            return OptimizedSharedMemoryHeader.unpack(header_bytes)
        except Exception as exc:
            logger.error(f"读取共享内存头部失败: {exc}")
            return None

    def _collect_idle_indices(self, header: OptimizedSharedMemoryHeader) -> List[int]:
        idle = []
        capacity = self.idle_queue_size // 4
        if capacity <= 0:
            return idle

        head = header.idle_queue_head
        tail = header.idle_queue_tail

        if head > tail:
            return idle

        span = min(tail - head + 1, capacity)

        for i in range(span):
            pos = (head + i) % capacity
            offset = self.idle_queue_offset + (pos * 4)
            raw = self.shared_memory.buf[offset:offset + 4]
            if len(raw) < 4:
                break
            value = struct.unpack('<I', raw)[0]
            idle.append(value)

        return idle

    def _read_descriptor(self, desc_index: int) -> Optional[OptimizedPacketDescriptor]:
        if desc_index < 0 or desc_index >= MAX_PACKETS:
            return None
        try:
            offset = self.descriptor_table_offset + desc_index * self.descriptor_size
            raw = bytes(self.shared_memory.buf[offset:offset + self.descriptor_size])
            if len(raw) < self.descriptor_size:
                return None
            return OptimizedPacketDescriptor.unpack(raw)
        except Exception as exc:
            logger.error(f"读取描述符失败: idx={desc_index}, err={exc}")
            return None
    
    def _create_lock_files(self):
        """创建文件锁文件"""
        import os
        
        # 创建/tmp目录（如果不存在）
        os.makedirs('/tmp', exist_ok=True)
        
        # 创建空闲队列锁文件
        idle_queue_lock = '/tmp/faasflow_idle_queue.lock'
        try:
            with open(idle_queue_lock, 'w') as f:
                f.write('')  # 创建空文件
            # 设置文件权限为666，确保容器可以访问
            os.chmod(idle_queue_lock, 0o666)
            # 设置文件所有者为root，确保容器可以访问
            os.chown(idle_queue_lock, 0, 0)  # root用户的UID和GID都是0
            logger.info(f"✓ 空闲队列锁文件创建成功: {idle_queue_lock}")
        except Exception as e:
            logger.error(f"创建空闲队列锁文件失败: {e}")
        
        # 创建位图锁文件（为每个字节创建一个锁文件）
        for byte_index in range(13):  # 13个字节的位图
            bitmap_lock = f'/tmp/faasflow_bitmap_{byte_index}.lock'
            try:
                with open(bitmap_lock, 'w') as f:
                    f.write('')  # 创建空文件
                # 设置文件权限为666，确保容器可以访问
                os.chmod(bitmap_lock, 0o666)
                # 设置文件所有者为root，确保容器可以访问
                os.chown(bitmap_lock, 0, 0)  # root用户的UID和GID都是0
                logger.info(f"✓ 位图锁文件创建成功: {bitmap_lock}")
            except Exception as e:
                logger.error(f"创建位图锁文件失败: {e}")
        
        logger.info("✓ 所有文件锁创建完成")
    
    def _cleanup_lock_files(self):
        """清理文件锁文件"""
        import os
        
        # 清理空闲队列锁文件
        idle_queue_lock = '/tmp/faasflow_idle_queue.lock'
        try:
            if os.path.exists(idle_queue_lock):
                os.remove(idle_queue_lock)
                logger.info(f"✓ 空闲队列锁文件清理成功: {idle_queue_lock}")
        except Exception as e:
            logger.error(f"清理空闲队列锁文件失败: {e}")
        
        # 清理位图锁文件
        for byte_index in range(13):  # 13个字节的位图
            bitmap_lock = f'/tmp/faasflow_bitmap_{byte_index}.lock'
            try:
                if os.path.exists(bitmap_lock):
                    os.remove(bitmap_lock)
                    logger.info(f"✓ 位图锁文件清理成功: {bitmap_lock}")
            except Exception as e:
                logger.error(f"清理位图锁文件失败: {e}")
        
        logger.info("✓ 所有文件锁清理完成")
    
    def cleanup(self):
        """清理共享内存池和文件锁"""
        self.stop_busy_monitor()
        if self.shared_memory and self._is_creator:
            try:
                # 清理文件锁
                self._cleanup_lock_files()
                # 清理共享内存
                self.shared_memory.unlink()
                logger.info(f"共享内存池 '{self.shm_name}' 清理完成")
            except Exception as e:
                logger.error(f"清理共享内存池失败: {e}")
        
        if self.shared_memory:
            try:
                self.shared_memory.close()
            except:
                pass
            self.shared_memory = None
        
        self._initialized = False
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        if not self._initialized:
            return {'initialized': False}
        
        return {
            'initialized': True,
            'version': 2,
            'shm_name': self.shm_name,
            'shm_size': self.shm_size,
            'max_packets': MAX_PACKETS,
            'data_area_size': self.data_area_size,
            'is_creator': self._is_creator
        }
    
    def cleanup(self):
        """清理共享内存资源"""
        self.stop_busy_monitor()
        if self.shared_memory:
            try:
                self.shared_memory.close()
                
                if self._is_creator:
                    try:
                        self.shared_memory.unlink()
                        logger.info(f"共享内存已删除: {self.shm_name}")
                    except FileNotFoundError:
                        pass
                        
            except Exception as e:
                logger.warning(f"清理共享内存时出现警告: {e}")
            finally:
                self.shared_memory = None
        
        self._initialized = False
    
    def __del__(self):
        """析构函数"""
        self.cleanup()


# ==============================================================================
# 测试代码
# ==============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    
    print("=" * 80)
    print("优化的共享内存管理器测试 - 宿主机端（初始化职责）")
    print("=" * 80)
    print()
    
    manager = OptimizedHostSharedMemoryManager('faasflow_shm_opt_test', SHM_SIZE)
    
    if manager.init_shm_pool():
        print("\n✓ 共享内存池初始化成功")
        print()
        
        # 显示统计信息
        stats = manager.get_stats()
        print("统计信息:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        print()
        print("=" * 80)
        print("测试说明：")
        print("  - 宿主机：只负责初始化共享内存（Header、描述符表、队列等）")
        print("  - 容器端：负责运行时的描述符分配、查找、回收、数据读写")
        print("  - 优势：消除Socket通信，使用idle队列+哈希表实现O(1)操作")
        print("=" * 80)
        print()
        
        # 清理
        print("清理共享内存...")
        manager.cleanup()
        print("✓ 测试完成")
    else:
        print("\n✗ 共享内存池初始化失败")

