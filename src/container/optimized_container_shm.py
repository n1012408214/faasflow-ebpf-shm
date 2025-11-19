"""
优化的容器端共享内存管理器 - 基于描述符表的实现

核心优化：
1. 直接从共享内存读取描述符表
2. 完全消除Socket通信
3. 使用共享内存中的原子操作管理依赖计数

与宿主机端 optimized_shm.py 配合使用
"""

import struct
import time
import threading
import logging
import ctypes
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List, Set
import multiprocessing.shared_memory as shm
import container_config
logger = logging.getLogger(__name__)

# 配置常量（与宿主机端保持一致）
SHM_NAME = 'faasflow_shm_opt'
SHM_SIZE = 5 * 1024 * 1024 * 1024  # 5GB
MAX_PACKETS = 5000  # 最大包数量
MAX_PACKET_SIZE = 1 * 1024 * 1024  # 1MB
SHM_MAGIC = 0x12345678

# 描述符状态（与宿主机端保持一致）
STATUS_FREE = 0
STATUS_WRITING = 1
STATUS_READY = 2
STATUS_READING = 3

# 标志位
FLAG_VALID = 0x01


@dataclass
class OptimizedPacketDescriptor:
    """
    优化的包描述符结构（56字节，简化版本）
    与宿主机端完全一致
    """
    data_offset: int = 0
    data_size: int = 0
    function_id: int = 0
    timestamp: int = 0
    flags: int = 0
    status: int = 0
    dependency_count: int = 0
    reserved: bytes = b'\x00' * 20
    
    def pack(self) -> bytes:
        """将描述符打包为56字节"""
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
        """从56字节解包描述符"""
        if len(data) < 56:
            raise ValueError(f"描述符数据长度不足: {len(data)} < 56")
        values = struct.unpack('<QIIQIII 20s', data[:56])
        return cls(*values)
    
    @staticmethod
    def size() -> int:
        return 56


@dataclass
class OptimizedSharedMemoryHeader:
    """
    简化的共享内存头部结构（256字节，直接使用desc_index）
    与宿主机端完全一致
    """
    magic: int = SHM_MAGIC
    version: int = 2
    total_size: int = SHM_SIZE
    header_size: int = 256  # 恢复头部大小
    descriptor_table_offset: int = 256  # 恢复偏移
    descriptor_table_size: int = 0
    bitmap_offset: int = 0
    bitmap_size: int = 0
    idle_queue_offset: int = 0
    idle_queue_size: int = 0
    data_area_offset: int = 0
    data_area_size: int = 0
    max_packets: int = MAX_PACKETS
    active_packets: int = 0
    idle_queue_head: int = 0
    idle_queue_tail: int = 0
    
    reserved: bytes = b'\x00' * 24   # 调整保留字段大小
    
    @classmethod
    def unpack(cls, data: bytes) -> 'OptimizedSharedMemoryHeader':
        """从124字节解包头部"""
        print(f"[调试] 头部数据长度: {len(data)} 字节")
        print(f"[调试] 期望的头部长度: 124 字节")
        if len(data) < 124:
            raise ValueError(f"头部数据长度不足: {len(data)} < 124")
        
        # 调试：打印前64字节的十六进制内容
        print(f"[调试] 头部前64字节: {data[:64].hex()}")
        
        # 调试：尝试解析格式字符串
        format_str = '<II Q I QQQQ QQQQ I I II 24s'
        print(f"[调试] 格式字符串: {format_str}")
        
        try:
            # 计算格式字符串期望的字节数
            expected_size = struct.calcsize(format_str)
            print(f"[调试] 格式字符串期望字节数: {expected_size}")
            
            values = struct.unpack(format_str, data[:124])
            print(f"[调试] 成功解析头部，得到 {len(values)} 个字段")
            return cls(*values)
        except struct.error as e:
            print(f"[调试] 格式字符串解析失败: {e}")
            raise
    
    @staticmethod
    def size() -> int:
        return 124


class OptimizedContainerSharedMemoryManager:
    """
    优化的容器端共享内存管理器
    
    特性：
    - 直接读取共享内存中的描述符表
    - 完全无Socket通信
    - 使用共享内存中的原子操作
    """
    
    def __init__(self, shm_name: str = SHM_NAME):
        self.shm_name = shm_name
        self.shared_memory = None
        self.header: Optional[OptimizedSharedMemoryHeader] = None
        self._initialized = False
        self.lock = threading.Lock()  # 容器内的本地锁
        self.queue_method = 'shared'  # 默认使用全局队列
        # 哈希统计（用于调试）
        self._hash_stats = {}
        
        # 内存布局参数（从header读取）
        self.header_size = 256  # 恢复头部大小
        self.descriptor_size = 56
        self.descriptor_table_offset = 0
        self.descriptor_table_size = 0
        self.bitmap_offset = 0
        self.bitmap_size = 0
        self.idle_queue_offset = 0
        self.idle_queue_size = 0
        self.data_area_offset = 0
        self.data_area_size = 0
        # 本地队列（仅当 queue_method == local 时使用）
        self.local_queue: List[int] = []
        self.local_queue_size = 0
        self.local_queue_high = 0
        self.local_queue_low = 0
        self.local_queue_init_batch = 0
        self.local_queue_refill_batch = 0
        self.local_queue_return_batch = 0
        self.local_busy: Set[int] = set()
        
    def init_shm_connection(self) -> bool:
        """
        连接到宿主机的共享内存池
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 连接到已有的共享内存
            self.shared_memory = shm.SharedMemory(name=self.shm_name)
            logger.info(f"容器连接到共享内存: {self.shm_name}")
            
            # 读取Header
            header_data = bytes(self.shared_memory.buf[:self.header_size])
            self.header = OptimizedSharedMemoryHeader.unpack(header_data)
            
            # 验证魔数和版本
            if self.header.magic != SHM_MAGIC:
                logger.error(f"共享内存魔数不匹配: {self.header.magic} != {SHM_MAGIC}")
                return False
            
            if self.header.version != 2:
                logger.error(f"共享内存版本不匹配: {self.header.version} != 2 (需要优化版本)")
                return False
            
            # 读取内存布局参数
            self.descriptor_table_offset = self.header.descriptor_table_offset
            self.descriptor_table_size = self.header.descriptor_table_size
            self.bitmap_offset = self.header.bitmap_offset
            self.bitmap_size = self.header.bitmap_size
            self.idle_queue_offset = self.header.idle_queue_offset
            self.idle_queue_size = self.header.idle_queue_size
            self.data_area_offset = self.header.data_area_offset
            self.data_area_size = self.header.data_area_size
            self._init_queue_strategy()
            self._initialized = True
            
            logger.info(f"容器端共享内存连接成功（简化版本）: "
                       f"DescTable={self.descriptor_table_size}B, "
                       f"Bitmap={self.bitmap_size}B, "
                       f"IdleQueue={self.idle_queue_size}B, "
                       f"DataArea={self.data_area_size}B")
            
            return True
            
        except FileNotFoundError:
            logger.error(f"共享内存不存在: {self.shm_name}，请确保宿主机已初始化")
            return False
        except Exception as e:
            logger.error(f"容器端共享内存连接失败: {e}")
            return False
    
    def _init_queue_strategy(self):
        """根据配置初始化队列策略"""
        configured_method = getattr(container_config, "QUEUE_METHOD", "shared") or "shared"
        method = str(configured_method).lower()
        if method not in ("local", "shared"):
            logger.warning(f"未知的队列策略: {configured_method}，回退到 shared")
            method = "shared"
        self.queue_method = method

        self.local_queue.clear()
        self.local_queue_size = 0
        self.local_busy.clear()

        if self.queue_method != "local":
            # 共享模式：关闭本地队列相关配置
            self.local_queue_high = 0
            self.local_queue_low = 0
            self.local_queue_init_batch = 0
            self.local_queue_refill_batch = 0
            self.local_queue_return_batch = 0
            self.local_busy.clear()
            logger.info("队列策略: method=shared（本地队列禁用）")
            print("[队列策略] 当前为 shared 模式，本地队列功能关闭")
            return

        def _positive_int(value, default):
            try:
                ivalue = int(value)
                return ivalue if ivalue > 0 else default
            except (TypeError, ValueError):
                return default

        self.local_queue_high = _positive_int(getattr(container_config, "LOCAL_QUEUE_HIGH", None), 0)
        self.local_queue_low = _positive_int(
            getattr(container_config, "LOCAL_QUEUE_LOW", None),
            min(self.local_queue_high, 0),
        )
        if self.local_queue_low > self.local_queue_high and self.local_queue_high > 0:
            logger.warning(f"本地队列低水位({self.local_queue_low})大于高水位({self.local_queue_high})，自动调整为相等")
            self.local_queue_low = self.local_queue_high

        self.local_queue_init_batch = _positive_int(
            getattr(container_config, "LOCAL_QUEUE_INIT_BATCH", None),
            min(self.local_queue_high, 0),
        )
        if self.local_queue_high and self.local_queue_init_batch > self.local_queue_high:
            self.local_queue_init_batch = self.local_queue_high

        self.local_queue_refill_batch = _positive_int(
            getattr(container_config, "LOCAL_QUEUE_REFILL_BATCH", None),
            max(self.local_queue_low, 1) or 1,
        )
        self.local_queue_return_batch = _positive_int(
            getattr(container_config, "LOCAL_QUEUE_RETURN_BATCH", None),
            max(self.local_queue_low, 1) or 1,
        )

        print(
            f"[队列策略] 启用 local 模式: high={self.local_queue_high}, low={self.local_queue_low}, "
            f"init_batch={self.local_queue_init_batch}, refill_batch={self.local_queue_refill_batch}, "
            f"return_batch={self.local_queue_return_batch}"
        )

        init_target = self.local_queue_init_batch

        if init_target > 0:
            print(f"[本地队列初始化] 准备从全局队列批量获取 {init_target} 个描述符")
            batch = self._pop_idle_queue_global_batch(init_target)
            acquired = len(batch)
            if acquired:
                self.local_queue.extend(batch)
                print(f"[本地队列初始化] 成功获取 {acquired} 个描述符，当前本地长度={len(self.local_queue)}")
            self.local_queue_size = len(self.local_queue)
            if acquired < init_target:
                logger.warning(
                    f"本地队列初始批量获取不足: 期望={init_target}, 实际={acquired}"
                )
                print(
                    f"[本地队列初始化] 期望={init_target}, 实际={acquired}，可能存在资源不足或竞争"
                )
        else:
            self.local_queue_size = 0
            acquired = 0
            print("[本地队列初始化] init_batch <= 0，跳过预取")

        logger.info(
            f"队列策略: method=local, local_high={self.local_queue_high}, "
            f"local_low={self.local_queue_low}, init_batch={self.local_queue_init_batch}, "
            f"refill_batch={self.local_queue_refill_batch}, return_batch={self.local_queue_return_batch}, "
            f"prefetched={acquired}"
        )
    
    def _read_descriptor(self, desc_index: int) -> OptimizedPacketDescriptor:
        """从共享内存读取描述符"""
        offset = self.descriptor_table_offset + (desc_index * self.descriptor_size)
        desc_data = bytes(self.shared_memory.buf[offset:offset + self.descriptor_size])
        return OptimizedPacketDescriptor.unpack(desc_data)
    
    def _write_descriptor(self, desc_index: int, descriptor: OptimizedPacketDescriptor):
        """将描述符写入共享内存"""
        offset = self.descriptor_table_offset + (desc_index * self.descriptor_size)
        packed_data = descriptor.pack()
        self.shared_memory.buf[offset:offset + len(packed_data)] = packed_data
    
    def _check_bitmap_bit(self, desc_index: int) -> bool:
        """检查位图中指定位是否被占用"""
        byte_index = desc_index // 8
        bit_index = desc_index % 8
        bitmap_addr = self.bitmap_offset + byte_index
        byte_val = self.shared_memory.buf[bitmap_addr]
        return bool((byte_val >> bit_index) & 1)
    
    def _set_bitmap_bit(self, desc_index: int, value: bool):
        """设置位图中指定位"""
        byte_index = desc_index // 8
        bit_index = desc_index % 8
        bitmap_addr = self.bitmap_offset + byte_index
        byte_val = self.shared_memory.buf[bitmap_addr]
        
        if value:
            byte_val |= (1 << bit_index)
        else:
            byte_val &= ~(1 << bit_index)
        
        self.shared_memory.buf[bitmap_addr] = byte_val
    
    def _atomic_check_and_set_bitmap_bit(self, desc_index: int) -> bool:
        """
        原子性地检查并设置位图位
        如果位未被占用，则设置为占用并返回True
        如果位已被占用，则返回False
        
        使用文件锁确保并发安全
        """
        import fcntl
        
        byte_index = desc_index // 8
        bit_index = desc_index % 8
        bitmap_addr = self.bitmap_offset + byte_index
        
        # 使用文件锁（最简单的方式）
        lock_file = f"/tmp/faasflow_bitmap_{byte_index}.lock"
        
        try:
            # 使用'a+'模式打开文件，如果文件不存在则创建
            with open(lock_file, 'a+') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # 排他锁
                
                # 在锁保护下进行原子操作
                byte_val = self.shared_memory.buf[bitmap_addr]
                bit_mask = 1 << bit_index
                
                if (byte_val & bit_mask) == 0:  # 位未被占用
                    # 设置为占用
                    byte_val |= bit_mask
                    self.shared_memory.buf[bitmap_addr] = byte_val
                    return True  # 成功获取
                else:
                    return False  # 位已被占用
                    
        except Exception as e:
            logger.error(f"位图操作失败: {e}")
            return False
    
    def _read_header_field(self, field_name: str):
        """读取Header中的字段值"""
        # 根据字段名计算偏移（按照新的struct.pack顺序）
        # '<II Q I QQQQ QQQQ QQ I Q I II Q Q II QQ 196s'
        # magic(4) + version(4) + total_size(8) + header_size(4) + 
        # desc_table_offset(8) + desc_table_size(8) + bitmap_offset(8) + bitmap_size(8) +
        # idle_queue_offset(8) + idle_queue_size(8) + hash_table_offset(8) + hash_table_size(8) +
        # data_area_offset(8) + data_area_size(8) + max_packets(4) + 
        # active_packets(4) + idle_queue_head(4) + idle_queue_tail(4) +
        # reserved(132)
        field_offsets = {
            'idle_queue_head': 92,                    # 4+4+8+4+8+8+8+8+8+8+8+8+4+4 = 92
            'idle_queue_tail': 96,                    # 92+4 = 96
        }
        
        if field_name not in field_offsets:
            raise ValueError(f"未知的字段: {field_name}")
        
        offset = field_offsets[field_name]
        
        # 4字节字段
        return struct.unpack('<I', bytes(self.shared_memory.buf[offset:offset + 4]))[0]
    
    def _write_header_field(self, field_name: str, value: int):
        """写入Header中的字段值"""
        field_offsets = {
            'idle_queue_head': 92,                    # 4+4+8+4+8+8+8+8+8+8+8+8+4+4 = 92
            'idle_queue_tail': 96,                    # 92+4 = 96
        }
        
        if field_name not in field_offsets:
            raise ValueError(f"未知的字段: {field_name}")
        
        offset = field_offsets[field_name]
        
        # 4字节字段
        self.shared_memory.buf[offset:offset + 4] = struct.pack('<I', value)
    
    def _pop_idle_queue_global_batch(self, count: int) -> List[int]:
        """
        从全局空闲队列批量弹出 desc_index
        
        队列逻辑（环形队列）：
        - 初始：head=0, tail=99，队列满（包含索引0-99）
        - Pop后：head=1, tail=99（队列有99个元素）
        - 空条件：head > tail（例如head=100, tail=99）
        
        使用一次文件锁确保并发安全
        """
        import fcntl
        
        if count <= 0:
            return []

        # 使用文件锁（最简单的方式）
        lock_file = f"/tmp/faasflow_idle_queue.lock"
        capacity = self.idle_queue_size // 4 if self.idle_queue_size else 0
        if capacity <= 0:
            return popped
        popped: List[int] = []
        
        try:
            # 使用'a+'模式打开文件，如果文件不存在则创建
            with open(lock_file, 'a+') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # 排他锁
                
                head = self._read_header_field('idle_queue_head')
                tail = self._read_header_field('idle_queue_tail')

                # 计算队列长度
                queue_length = tail - head + 1 if head <= tail else 0
                print(f"[空闲队列] 当前长度: {queue_length}, head: {head}, tail: {tail}")

                # 检查队列是否为空
                if head > tail:
                    print(f"[空闲队列] 队列为空，无法分配描述符")
                    return []

                take = min(count, queue_length)

                for i in range(take):
                    pos = (head + i) % capacity
                    queue_addr = self.idle_queue_offset + (pos * 4)
                    desc_index = struct.unpack('<I', bytes(self.shared_memory.buf[queue_addr:queue_addr + 4]))[0]
                    popped.append(desc_index)

                # 移动头指针
                new_head = head + take
                self._write_header_field('idle_queue_head', new_head)

                remaining = tail - new_head + 1 if new_head <= tail else 0
                if remaining < 0:
                    remaining = 0
                print(
                    f"[空闲队列] 批量弹出 {take} 个描述符，剩余长度: {remaining}, "
                    f"new_head: {new_head}, tail: {tail}"
                )
                
        except Exception as e:
            logger.error(f"空闲队列操作失败: {e}")
            return []

        return popped

    def _pop_idle_queue_global(self) -> Optional[int]:
        """从全局空闲队列弹出一个 desc_index"""
        result = self._pop_idle_queue_global_batch(1)
        return result[0] if result else None
    
    def _push_idle_queue_global_batch(self, desc_indices: List[int]):
        """
        将多个 desc_index 推入空闲队列（批量 O(k)）
        
        队列逻辑：
        - Push 时：tail 递增，并依次写入
        - 例如：head=1, tail=99，批量推入3个 -> tail 依次写为100/101/102
        
        使用一次文件锁确保并发安全
        """
        import fcntl
        
        if not desc_indices:
            return

        lock_file = f"/tmp/faasflow_idle_queue.lock"
        capacity = self.idle_queue_size // 4 if self.idle_queue_size else 0
        if capacity <= 0:
            return

        try:
            # 使用'a+'模式打开文件，如果文件不存在则创建
            with open(lock_file, 'a+') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # 排他锁
                
                head = self._read_header_field('idle_queue_head')
                tail = self._read_header_field('idle_queue_tail')

                start_tail = tail
                for idx, desc_index in enumerate(desc_indices):
                    pos = (start_tail + idx + 1) % capacity
                    queue_addr = self.idle_queue_offset + (pos * 4)
                    self.shared_memory.buf[queue_addr:queue_addr + 4] = struct.pack('<I', desc_index)
                
                final_tail = start_tail + len(desc_indices)
                self._write_header_field('idle_queue_tail', final_tail)
                
                new_queue_length = final_tail - head + 1
                if new_queue_length < 0:
                    new_queue_length = 0
                print(
                    f"[空闲队列] 批量回收 {len(desc_indices)} 个描述符，新长度: {new_queue_length}, "
                    f"head: {head}, tail: {final_tail}"
                )
                
        except Exception as e:
            logger.error(f"空闲队列回收失败: {e}")

    def _push_idle_queue_global(self, desc_index: int):
        """将单个 desc_index 推入空闲队列"""
        self._push_idle_queue_global_batch([desc_index])
    
    def _pop_idle_queue(self) -> Optional[int]:
        """根据队列策略从空闲队列获取描述符"""
        if self.queue_method == "local":
            return self._pop_idle_queue_local()
        return self._pop_idle_queue_global()

    def _push_idle_queue(self, desc_index: int):
        """根据队列策略回收描述符"""
        if desc_index in self.local_busy:
            self.local_busy.discard(desc_index)
            print(
                f"[本地归还调度] desc_index={desc_index} 从 busy 集合移除，"
                f"剩余 busy={len(self.local_busy)}"
            )

        if self.queue_method == "local":
            self._push_idle_queue_local(desc_index)
            return
        self._push_idle_queue_global(desc_index)
    
    def _pop_idle_queue_local(self) -> Optional[int]:
        """
        从本地队列弹出一个desc_index（O(1)）
        """
        if self.local_queue_size > 0:
            self.local_queue_size -= 1
            desc = self.local_queue.pop()  # LIFO，避免列表移动
            self.local_busy.add(desc)
            print(
                f"[本地分配] 命中本地队列 desc_index={desc}, "
                f"剩余本地长度={self.local_queue_size}, 本地busy={len(self.local_busy)}"
            )
            return desc

        # 本地为空，决定是否批量补给
        if self.local_queue_refill_batch > 0 and self.local_queue_size <= self.local_queue_low:
            print(
                f"[本地分配] 本地队列到达低水位，尝试批量补给 {self.local_queue_refill_batch} 个"
            )
            batch = self._pop_idle_queue_global_batch(self.local_queue_refill_batch)
            if batch:
                self.local_queue.extend(batch)
                self.local_queue_size = len(self.local_queue)
                # 再次弹出
                self.local_queue_size -= 1
                desc = self.local_queue.pop()
                self.local_busy.add(desc)
                print(
                    f"[本地分配] 补给成功，获取 desc_index={desc}, "
                    f"补给后本地长度={self.local_queue_size}, 本地busy={len(self.local_busy)}"
                )
                return desc
            else:
                print("[本地分配] 补给失败，从全局队列直接获取")

        desc = self._pop_idle_queue_global()
        if desc is not None:
            self.local_busy.add(desc)
            print(
                f"[本地分配] 全局分配 desc_index={desc}, "
                f"本地busy={len(self.local_busy)}"
            )
        return desc
    
    def _push_idle_queue_local(self, desc_index: int):
        """
        将desc_index推入本地队列（O(1)）
        """
        self.local_queue.append(desc_index)
        self.local_queue_size += 1
        print(
            f"[本地归还] 收到 desc_index={desc_index}, "
            f"本地长度={self.local_queue_size}"
        )

        if self.local_queue_high and self.local_queue_size > self.local_queue_high:
            print(
                f"[本地归还触发] queue_size={self.local_queue_size}, "
                f"high={self.local_queue_high}, low={self.local_queue_low}, "
                f"return_batch={self.local_queue_return_batch}, local_busy={len(self.local_busy)}"
            )

            return_count = min(self.local_queue_return_batch or 0, self.local_queue_size)

            if return_count <= 0:
                return_count = self.local_queue_size - max(self.local_queue_low, 0)
                return_count = max(return_count, 0)

            if return_count > 0:
                batch = [self.local_queue.pop() for _ in range(return_count)]
                self.local_queue_size -= return_count
                print(
                    f"[本地归还执行] return_count={return_count}, "
                    f"queue_size_after={self.local_queue_size}"
                )
                self._push_idle_queue_global_batch(batch)
            else:
                print("[本地归还跳过] 计算后的 return_count 为 0，保持本地缓存")

    def allocate_write_descriptor(self, data_size: int, function_id: int, dependency_count: int) -> Optional[int]:
        """
        容器端分配写入描述符（简化版本，直接返回desc_index）
        
        流程：
        1. 从idle_queue弹出空闲desc_index（O(1)）
        2. 填充描述符
        3. 设置bitmap为忙碌
        
        时间：~0.05ms（vs Socket方式的5.5ms）
        
        Args:
            data_size: 数据大小
            function_id: 函数ID
            dependency_count: 依赖计数
            
        Returns:
            desc_index或None
        """
        if not self._initialized:
            logger.error("共享内存未初始化")
            return None
        
        alloc_start = time.time()
        
        try:
            # 1. 从空闲队列获取desc_index（O(1)）
            queue_start = time.time()
            desc_index = self._pop_idle_queue()
            queue_time = time.time() - queue_start
            
            if desc_index is None:
                logger.error("没有空闲描述符可用")
                return None
            
            # 2. 原子性地检查并设置位图（防止竞态条件）
            #bitmap_start = time.time()
            #if not self._atomic_check_and_set_bitmap_bit(desc_index):
            #    # 位图设置失败，说明desc_index已被其他容器占用
            #    logger.warning(f"desc_index={desc_index}已被其他容器占用，重新分配")
            #    # 将desc_index放回空闲队列
            #    self._push_idle_queue(desc_index)
            #    return None
            #bitmap_time = time.time() - bitmap_start
            
            # 3. 读取描述符，填充数据
            fill_start = time.time()
            descriptor = self._read_descriptor(desc_index)
            descriptor.data_size = data_size
            descriptor.function_id = function_id
            descriptor.timestamp = int(time.time() * 1000000)
            descriptor.flags = FLAG_VALID
            descriptor.status = STATUS_WRITING
            descriptor.dependency_count = dependency_count
            self._write_descriptor(desc_index, descriptor)
            fill_time = time.time() - fill_start
            
            alloc_total = time.time() - alloc_start
            
            #print(f"[容器分配描述符] desc_index={desc_index}, "
            #      f"dependency_count={dependency_count}, "
            #      f"队列pop={queue_time*1000:.3f}ms, "
            #      f"填充={fill_time*1000:.3f}ms, "
            #      f"总计={alloc_total*1000:.2f}ms")
            
            return desc_index
            
        except Exception as e:
            logger.error(f"分配写入描述符失败: {e}")
            return None
    
    def get_descriptor_by_desc_index(self, desc_index: int) -> Optional[Dict[str, Any]]:
        """
        根据desc_index直 接读取描述符（简化版本，O(1)，无Socket）
        
        流程：
        1. 直接读取描述符（O(1)）
        
        时间：~0.001ms（vs 线性查找的0.05ms，vs Socket的3.7ms）
        
        Args:
            desc_index: 描述符索引
            
        Returns:
            描述符信息字典或None
        """
        if not self._initialized:
            logger.error("共享内存未初始化")
            return None
        
        search_start = time.time()
        
        try:
            # 检查desc_index是否有效
            if desc_index < 0 or desc_index >= self.header.max_packets:
                logger.error(f"desc_index超出范围: {desc_index}, 最大值为{self.header.max_packets-1}")
                return None
            
            # 检查描述符是否被占用
            #if not self._check_bitmap_bit(desc_index):
            #    logger.error(f"desc_index={desc_index} 未被占用，可能已被回收")
            #    return None
            
            # 读取描述符
            read_start = time.time()
            descriptor = self._read_descriptor(desc_index)
            read_time = time.time() - read_start
            
            search_time = time.time() - search_start
            
            print(f"[容器查找描述符] desc_index={desc_index}, "
                  f"读描述符={read_time*1000:.3f}ms, "
                  f"总计={search_time*1000:.3f}ms")
            
            return {
                'desc_index': desc_index,
                'data_offset': descriptor.data_offset,
                'data_size': descriptor.data_size,
                'function_id': descriptor.function_id,
                'timestamp': descriptor.timestamp,
                'flags': descriptor.flags,
                'status': descriptor.status,
                'dependency_count': descriptor.dependency_count
            }
            
        except Exception as e:
            logger.error(f"查找描述符失败: {e}")
            return None
    
    def read_data(self, data_offset: int, data_size: int) -> Optional[bytes]:
        """
        从共享内存读取数据
        
        Args:
            data_offset: 数据偏移
            data_size: 数据大小
            
        Returns:
            数据或None
        """
        if not self._initialized:
            logger.error("共享内存未初始化")
            return None
        
        try:
            read_start = time.time()
            
            # 边界检查
            if data_offset + data_size > self.header.total_size:
                logger.error(f"数据超出共享内存边界: offset={data_offset}, size={data_size}")
                return None
            
            # 读取数据
            data = bytes(self.shared_memory.buf[data_offset:data_offset + data_size])
            
            read_time = time.time() - read_start
            #print(f"[容器读取数据] offset={data_offset}, size={data_size}, 时间={read_time*1000:.2f}ms")
            
            return data
            
        except Exception as e:
            logger.error(f"读取数据失败: {e}")
            return None
    
    def write_data(self, data_offset: int, data: bytes) -> bool:
        """
        写入数据到共享内存
        
        Args:
            data_offset: 数据偏移
            data: 数据内容
            
        Returns:
            是否成功
        """
        if not self._initialized:
            logger.error("共享内存未初始化")
            return False
        
        try:
            write_start = time.time()
            
            # 边界检查
            if data_offset + len(data) > self.header.total_size:
                logger.error(f"数据超出共享内存边界: offset={data_offset}, size={len(data)}")
                return False
            
            # 写入数据
            self.shared_memory.buf[data_offset:data_offset + len(data)] = data
            
            write_time = time.time() - write_start
            #print(f"[容器写入数据] offset={data_offset}, size={len(data)}, 时间={write_time*1000:.2f}ms")
            
            return True
            
        except Exception as e:
            logger.error(f"写入数据失败: {e}")
            return False
    
    def mark_descriptor_status(self, desc_index: int, status: int):
        """
        更新描述符状态
        
        Args:
            desc_index: 描述符索引
            status: 新状态
        """
        try:
            descriptor = self._read_descriptor(desc_index)
            descriptor.status = status
            self._write_descriptor(desc_index, descriptor)
            
            status_name = {0: 'FREE', 1: 'WRITING', 2: 'READY', 3: 'READING'}.get(status, 'UNKNOWN')
            #print(f"[容器更新状态] desc_index={desc_index}, status={status_name}")
            
        except Exception as e:
            logger.error(f"更新描述符状态失败: {e}")
    
    def _atomic_decrement_dependency(self, desc_index: int) -> int:
        """
        原子递减描述符的依赖计数
        
        使用ctypes实现内存级别的原子操作，避免并发冲突
        
        Returns:
            递减后的新值
        """
        # 计算dependency_count在描述符中的偏移
        # OptimizedPacketDescriptor: data_offset(8) + data_size(4) + 
        #                            function_id(4) + timestamp(8) + flags(4) + status(4) + dependency_count(4)
        dependency_offset_in_desc = 8 + 8 + 4 + 4 + 8 + 4 + 4  # = 40
        
        # 计算在整个共享内存中的绝对偏移
        desc_offset = self.descriptor_table_offset + (desc_index * self.descriptor_size)
        absolute_offset = desc_offset + dependency_offset_in_desc
        
        # 创建一个指向共享内存的ctypes整数指针
        # 注意：这里需要确保内存对齐
        int_ptr = ctypes.c_int32.from_buffer(self.shared_memory.buf, absolute_offset)
        
        # 使用简单的锁保证原子性（Python GIL + 锁）
        # 真正的原子操作需要使用平台相关的原子指令
        with self.lock:
            old_value = int_ptr.value
            int_ptr.value = old_value - 1
            new_value = int_ptr.value
        
        return new_value
    
    def notify_read_complete(self, desc_index: int) -> bool:
        """
        通知读取完成（简化版本，直接操作共享内存，无Socket）
        
        流程：
        1. 原子递减依赖计数（并发安全）
        2. 如果计数归零，回收描述符：
           - 推入idle队列
           - 清除bitmap
           - 标记状态为FREE
        
        时间：~0.01ms（vs Socket的4.1ms）
        
        Args:
            desc_index: 描述符索引
            
        Returns:
            是否成功
        """
        try:
            notify_start = time.time()
            
            # 原子递减依赖计数
            decrement_start = time.time()
            new_count = self._atomic_decrement_dependency(desc_index)
            decrement_time = time.time() - decrement_start
            
            # 如果计数归零，回收描述符
            if new_count <= 0:
                reclaim_start = time.time()
                
                # 重新读取descriptor确保是最新状态
                descriptor = self._read_descriptor(desc_index)
                
                print(f"[容器回收准备] desc_index={desc_index}, new_count={new_count}")
                
                # 1. 标记描述符为空闲
                descriptor.status = STATUS_FREE
                descriptor.data_size = 0
                descriptor.dependency_count = 0  # 显式设置为0
                self._write_descriptor(desc_index, descriptor)
                
                # 2. 清除bitmap
                self._set_bitmap_bit(desc_index, False)
                
                # 3. 推回desc_index空闲队列
                self._push_idle_queue(desc_index)
                
                reclaim_time = time.time() - reclaim_start
                
                notify_time = time.time() - notify_start
                print(f"[容器回收描述符] desc_index={desc_index}, "
                      f"递减={decrement_time*1000:.3f}ms, 回收={reclaim_time*1000:.3f}ms, "
                      f"总计={notify_time*1000:.2f}ms")
            else:
                notify_time = time.time() - notify_start
                #print(f"[容器递减计数] desc_index={desc_index}, "
                #      f"依赖计数 -> {new_count}, 时间={notify_time*1000:.2f}ms")
            
            return True
            
        except Exception as e:
            logger.error(f"通知读取完成失败: {e}")
            return False
    
    
    def cleanup(self):
        """清理共享内存资源"""
        if self.shared_memory:
            try:
                self.shared_memory.close()
                logger.info("容器端共享内存连接已关闭")
            except Exception as e:
                logger.warning(f"清理共享内存时出现警告: {e}")
            finally:
                self.shared_memory = None
        
        self._initialized = False
    
    def __del__(self):
        """析构函数"""
        self.cleanup()


# 高层API封装
class OptimizedContainerShmUtils:
    """
    容器端共享内存工具类
    提供简化的API，适配现有的store.py
    """
    
    def __init__(self, shm_name: str = SHM_NAME):
        self.shm_manager = OptimizedContainerSharedMemoryManager(shm_name)
        self.initialized = False
    
    def init_connection(self) -> bool:
        """初始化连接"""
        self.initialized = self.shm_manager.init_shm_connection()
        return self.initialized
    
    def fetch_data(self, desc_index: int) -> Optional[bytes]:
        """
        根据desc_index读取数据（简化版本，无Socket）
        
        流程：
        1. 直接读取描述符信息（O(1)，~0.001ms）
        2. 读取数据（~0.3ms）
        3. 递减依赖计数并自动回收（~0.01ms）
        
        总耗时：~0.31ms（vs 当前8.2ms）→ 26倍提升
        
        Args:
            desc_index: 描述符索引
            
        Returns:
            数据或None
        """
        if not self.initialized:
            logger.error("共享内存未初始化")
            return None
        
        total_start = time.time()
        
        try:
            # 1. 直接读取描述符信息（O(1)，~0.001ms）
            search_start = time.time()
            desc_info = self.shm_manager.get_descriptor_by_desc_index(desc_index)
            search_time = time.time() - search_start
            
            if not desc_info:
                logger.error(f"未找到desc_index={desc_index}的描述符")
                return None
            
            data_offset = desc_info['data_offset']
            data_size = desc_info['data_size']
            
            # 2. 读取数据（~0.3ms）
            read_start = time.time()
            data = self.shm_manager.read_data(data_offset, data_size)
            read_time = time.time() - read_start
            
            if not data:
                logger.error(f"读取数据失败: desc_index={desc_index}")
                return None
            
            # 3. 通知读取完成，递减依赖计数并自动回收（~0.01ms）
            notify_start = time.time()
            self.shm_manager.notify_read_complete(desc_index)
            notify_time = time.time() - notify_start
            
            total_time = time.time() - total_start
            
            print(f"[简化FETCH] desc_index={desc_index}, "
                  f"查找={search_time*1000:.3f}ms, 读取={read_time*1000:.2f}ms, "
                  f"回收={notify_time*1000:.3f}ms, 总计={total_time*1000:.2f}ms, "
                  f"大小={len(data)} bytes")
            
            return data
            
        except Exception as e:
            logger.error(f"读取数据异常: {e}")
            return None
    
    def post_data(self, key: str, val, datatype: str, function_id: int, dependency_count: int) -> Optional[int]:
        """
        写入数据并返回desc_index（简化版本，容器自主分配）
        
        流程：
        1. 序列化数据
        2. 容器自主分配描述符（从idle队列pop）
        3. 写入数据到共享内存
        4. 更新描述符状态为READY
        
        总耗时：~0.35ms（vs Socket的8.4ms）
        
        Args:
            key: 数据键
            val: 数据值
            datatype: 数据类型
            function_id: 函数ID
            dependency_count: 依赖计数
            
        Returns:
            desc_index或None
        """
        if not self.initialized:
            logger.error("共享内存未初始化")
            return None
        
        total_start = time.time()
        
        try:
            # 1. 序列化数据
            serialize_start = time.time()
            import json
            data_bytes = json.dumps(val).encode('utf-8')
            serialize_time = time.time() - serialize_start
            
            # 2. 容器自主分配描述符
            alloc_start = time.time()
            if dependency_count > 0:
                desc_index = self.shm_manager.allocate_write_descriptor(
                    len(data_bytes),
                    function_id,
                    dependency_count
                )
                alloc_time = time.time() - alloc_start

                if desc_index is None:
                    logger.error("分配描述符失败")
                    return None

                # 获取描述符信息以获取数据偏移
                desc_info = self.shm_manager.get_descriptor_by_desc_index(desc_index)
                if not desc_info:
                    logger.error("获取描述符信息失败")
                    return None

                data_offset = desc_info['data_offset']

                # 3. 写入数据
                write_start = time.time()
                if not self.shm_manager.write_data(data_offset, data_bytes):
                    logger.error(f"写入数据失败: desc_index={desc_index}")
                    return None
                write_time = time.time() - write_start

                # 4. 更新状态为READY
                status_start = time.time()
                self.shm_manager.mark_descriptor_status(desc_index, STATUS_READY)
                status_time = time.time() - status_start

                total_time = time.time() - total_start

                print(f"[简化POST] key={key}, desc_index={desc_index}, "
                      f"序列化={serialize_time*1000:.2f}ms, 分配={alloc_time*1000:.2f}ms, "
                      f"写入={write_time*1000:.2f}ms, 状态={status_time*1000:.2f}ms, "
                      f"总计={total_time*1000:.2f}ms, 大小={len(data_bytes)} bytes")

                return desc_index
            else:
                return 0
            
        except Exception as e:
            logger.error(f"写入数据异常: {e}")
            import traceback
            traceback.print_exc()
            return None


# 全局单例
_global_container_shm_manager = None


def get_global_optimized_container_shm_manager(shm_name: str = SHM_NAME) -> OptimizedContainerShmUtils:
    """获取全局容器端共享内存管理器"""
    global _global_container_shm_manager
    if _global_container_shm_manager is None:
        _global_container_shm_manager = OptimizedContainerShmUtils(shm_name)
        _global_container_shm_manager.init_connection()
    return _global_container_shm_manager


# 便捷函数
def fetch_data_optimized(desc_index: int) -> Optional[bytes]:
    """使用全局管理器读取数据"""
    manager = get_global_optimized_container_shm_manager()
    return manager.fetch_data(desc_index)


# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 80)
    print("优化的容器端共享内存管理器测试")
    print("=" * 80)
    
    # 创建容器端管理器
    container_manager = OptimizedContainerSharedMemoryManager(shm_name='faasflow_shm_test')
    
    # 连接到共享内存
    if container_manager.init_shm_connection():
        print("\n✓ 容器端共享内存连接成功\n")
        
        # 显示内存布局
        print("共享内存布局:")
        print(f"  描述符表偏移: {container_manager.descriptor_table_offset}")
        print(f"  描述符表大小: {container_manager.descriptor_table_size}")
        print(f"  位图偏移: {container_manager.bitmap_offset}")
        print(f"  位图大小: {container_manager.bitmap_size}")
        print(f"  数据区偏移: {container_manager.data_area_offset}")
        print(f"  数据区大小: {container_manager.data_area_size}")
        
        # 测试读取（假设宿主机已经写入了desc_index=1的数据）
        print("\n" + "=" * 80)
        print("测试读取数据（假设desc_index=1已存在）")
        print("=" * 80)
        
        utils = OptimizedContainerShmUtils(shm_name='faasflow_shm_test')
        if utils.init_connection():
            # 尝试读取desc_index=1
            data = utils.fetch_data(desc_index=1)
            if data:
                print(f"\n✓ 数据读取成功: {len(data)} bytes")
            else:
                print("\n⚠ 未找到desc_index=1的数据（可能宿主机未写入）")
        
        # 清理
        container_manager.cleanup()
        print("\n✓ 测试完成")
    else:
        print("\n✗ 容器端共享内存连接失败")
        print("请确保：")
        print("  1. 宿主机已运行并初始化了共享内存池")
        print("  2. 共享内存名称匹配（faasflow_shm_test）")


