#!/usr/bin/env python3
"""
基于multiprocessing.shared_memory的共享内存管理器
使用Python标准库实现，更加稳定可靠
"""

import multiprocessing.shared_memory as shm
import threading
import time
import logging
import struct
import json
from typing import Optional, Tuple, Dict, Any
from dataclasses import dataclass
from enum import IntEnum

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 错误码定义
class ErrorCode(IntEnum):
    SUCCESS = 0
    ERROR_SHM_INIT = -1
    ERROR_SHM_ATTACH = -2
    ERROR_PACKET_FULL = -3
    ERROR_INVALID_PACKET = -4

# 包标志位
class PacketFlags(IntEnum):
    VALID = 0x00000001
    PROCESSED = 0x00000002
    ERROR = 0x00000004

# 配置常量
SHM_SIZE = 5 * 1024 * 1024 * 1024   # 5GB，增加共享内存大小
MAX_PACKETS = 5000
MAX_PACKET_SIZE = 1 * 1024 * 1024  # 1MB，支持更大的数据包
SHM_MAGIC = 0x12345678

@dataclass
class PacketDescriptor:
    """包描述符结构"""
    packet_id: int = 0
    data_offset: int = 0
    data_size: int = 0
    function_id: int = 0
    timestamp: int = 0
    flags: int = 0
    
    def pack(self) -> bytes:
        """将描述符打包为字节"""
        return struct.pack('<QQIIQI',
                          self.packet_id,
                          self.data_offset,
                          self.data_size,
                          self.function_id,
                          self.timestamp,
                          self.flags)
    
    @classmethod
    def unpack(cls, data: bytes) -> 'PacketDescriptor':
        """从字节解包描述符"""
        values = struct.unpack('<QQIIQI', data)
        return cls(*values)

@dataclass
class SharedMemoryHeader:
    """共享内存头部结构"""
    magic: int = SHM_MAGIC
    version: int = 1
    total_size: int = SHM_SIZE
    data_offset: int = 0
    data_size: int = 0
    packet_count: int = 0
    next_packet_id: int = 1
    
    def pack(self) -> bytes:
        """将头部打包为字节"""
        # 使用Q格式符处理大整数，确保不会溢出
        return struct.pack('<IIQQQQQ',
                          self.magic,
                          self.version,
                          self.total_size,
                          self.data_offset,
                          self.data_size,
                          self.packet_count,
                          self.next_packet_id)
    
    @classmethod
    def unpack(cls, data: bytes) -> 'SharedMemoryHeader':
        """从字节解包头部"""
        values = struct.unpack('<IIQQQQQ', data)
        return cls(*values)

class HostSharedMemoryManager:
    """
    宿主机共享内存管理器
    负责：初始化shm池，维护idle和busy描述符map，管理依赖计数
    """
    
    def __init__(self, shm_name: str = "faasflow_shm", shm_size: int = SHM_SIZE):
        """
        初始化宿主机共享内存管理器
        
        Args:
            shm_name: 共享内存名称
            shm_size: 共享内存大小（字节）
        """
        self.shm_name = shm_name
        self.shm_size = shm_size
        self.shared_memory = None
        self.header = None
        self.lock = threading.RLock()
        self._initialized = False
        self._is_creator = False
        
        # 简化的内存布局：只有头部和数据区域
        self.header_size = len(SharedMemoryHeader().pack())
        self.data_offset = self.header_size  # 数据区域紧跟在头部后面
        
        # 本地描述符管理
        self.idle_descriptors = set(range(MAX_PACKETS))  # 空闲描述符索引
        self.busy_descriptors = {}  # {packet_id: (desc_index, PacketDescriptor)}
        self.dependency_count = {}  # {packet_id: count} 依赖计数
        self.next_packet_id = 1
        
        # Socket通信
        self.socket_server = None
        self.socket_thread = None
        
        logger.info(f"宿主机共享内存管理器初始化: 大小={shm_size}")
    
    
    def init_shm_pool(self) -> bool:
        """
        初始化共享内存池（宿主机版本）
        
        Returns:
            bool: 初始化是否成功
        """
        if self._initialized:
            logger.warning("共享内存池已经初始化")
            return True
        
        try:
            with self.lock:
                # 尝试连接到现有的共享内存
                try:
                    self.shared_memory = shm.SharedMemory(name=self.shm_name)
                    logger.info("连接到现有共享内存池")
                    self._is_creator = False
                except FileNotFoundError:
                    # 创建新的共享内存
                    self.shared_memory = shm.SharedMemory(name=self.shm_name, create=True, size=self.shm_size)
                    logger.info("创建新的共享内存池")
                    self._is_creator = True
                
                # 检查是否是新创建的共享内存
                header_data = bytes(self.shared_memory.buf[:self.header_size])
                self.header = SharedMemoryHeader.unpack(header_data)
                
                if self.header.magic != SHM_MAGIC:
                    logger.info("初始化新的共享内存池")
                    
                    # 初始化头部（简化版本）
                    self.header = SharedMemoryHeader(
                        data_offset=self.data_offset,
                        data_size=self.shm_size - self.data_offset
                    )
                    
                    # 写入头部
                    self.shared_memory.buf[:self.header_size] = self.header.pack()
                    
                    logger.info(f"共享内存池初始化完成: 数据偏移={self.data_offset}, 数据大小={self.header.data_size}")
                else:
                    # 使用现有的数据偏移量
                    self.data_offset = self.header.data_offset
                    logger.info(f"使用现有数据偏移量: {self.data_offset}")
                
                # 先设置_initialized为True，再启动Socket服务器
                self._initialized = True
                
                # 启动Socket服务器
                self._start_socket_server()
                logger.info(f"宿主机共享内存池信息: 总大小={self.header.total_size}, "
                           f"数据区域={self.header.data_size}")
                
                return True
                
        except Exception as e:
            logger.error(f"初始化共享内存池时发生异常: {e}")
            self._cleanup()
            return False
    
    def _start_socket_server(self):
        """启动Socket服务器监听容器请求"""
        import socket
        import threading
        
        def socket_server_thread():
            try:
                print(f"[宿主机SHM调试] 开始创建Socket服务器")
                # 创建Unix socket服务器
                self.socket_server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.socket_server.bind('/tmp/faasflow_host_shm.sock')
                self.socket_server.listen(10)
                
                logger.info("宿主机Socket服务器启动: /tmp/faasflow_host_shm.sock")
                print(f"[宿主机SHM调试] Socket服务器启动完成，_initialized={self._initialized}")
                
                while self._initialized:
                    try:
                        # 设置超时，避免无限阻塞
                        self.socket_server.settimeout(1.0)
                        client_socket, _ = self.socket_server.accept()
                        # 为每个连接创建处理线程
                        threading.Thread(
                            target=self._handle_container_request,
                            args=(client_socket,),
                            daemon=True
                        ).start()
                    except socket.timeout:
                        # 超时是正常的，继续循环
                        continue
                    except Exception as e:
                        if self._initialized:
                            logger.error(f"接受容器连接失败: {e}")
                            print(f"[宿主机SHM调试] 接受连接异常: {e}")
                        
            except Exception as e:
                logger.error(f"启动Socket服务器失败: {e}")
        
        self.socket_thread = threading.Thread(target=socket_server_thread, daemon=True)
        self.socket_thread.start()
    
    def _handle_container_request(self, client_socket):
        """处理容器请求"""
        import json
        import struct
        
        try:
            print(f"[宿主机SHM调试] 收到容器连接请求")
            # 读取请求类型
            request_type = client_socket.recv(4)
            print(f"[宿主机SHM调试] 接收请求类型数据: {request_type}")
            if len(request_type) < 4:
                print(f"[宿主机SHM调试] 请求类型数据不足: {len(request_type)}")
                return
            
            request_type = struct.unpack('!I', request_type)[0]
            print(f"[宿主机SHM调试] 解析请求类型: {request_type}")
            
            if request_type == 1:  # 请求读取描述符
                print(f"[宿主机SHM调试] 处理读取描述符请求")
                self._handle_read_request(client_socket)
            elif request_type == 2:  # 请求写入描述符
                print(f"[宿主机SHM调试] 处理写入描述符请求")
                self._handle_write_request(client_socket)
            elif request_type == 3:  # 通知读取完成
                print(f"[宿主机SHM调试] 处理读取完成通知")
                self._handle_read_complete(client_socket)
            elif request_type == 4:  # 通知写入完成
                print(f"[宿主机SHM调试] 处理写入完成通知")
                self._handle_write_complete(client_socket)
            else:
                print(f"[宿主机SHM调试] 未知请求类型: {request_type}")
                
        except Exception as e:
            logger.error(f"处理容器请求失败: {e}")
            print(f"[宿主机SHM调试] 处理容器请求异常: {e}")
        finally:
            print(f"[宿主机SHM调试] 关闭容器连接")
            client_socket.close()
    
    def _handle_read_request(self, client_socket):
        """处理读取描述符请求"""
        import json
        import struct
        
        try:
            # 读取请求数据
            data_len = struct.unpack('!I', client_socket.recv(4))[0]
            request_data = client_socket.recv(data_len)
            request = json.loads(request_data.decode('utf-8'))
            
            packet_id = request['packet_id']
            
            with self.lock:
                if packet_id in self.busy_descriptors:
                    desc_index, descriptor = self.busy_descriptors[packet_id]
                    
                    # 发送描述符给容器
                    descriptor_data = {
                        'packet_id': descriptor.packet_id,
                        'data_offset': descriptor.data_offset,
                        'data_size': descriptor.data_size,
                        'function_id': descriptor.function_id,
                        'timestamp': descriptor.timestamp,
                        'flags': descriptor.flags
                    }
                    
                    response = json.dumps(descriptor_data).encode('utf-8')
                    client_socket.sendall(struct.pack('!I', len(response)))
                    client_socket.sendall(response)
                    
                    logger.info(f"发送读取描述符: packet_id={packet_id}")
                else:
                    # 发送错误响应
                    error_response = json.dumps({'error': 'packet_not_found'}).encode('utf-8')
                    client_socket.sendall(struct.pack('!I', len(error_response)))
                    client_socket.sendall(error_response)
                    
        except Exception as e:
            logger.error(f"处理读取请求失败: {e}")
    
    def _handle_write_request(self, client_socket):
        """处理写入描述符请求"""
        import json
        import struct
        
        try:
            # 读取请求数据
            data_len = struct.unpack('!I', client_socket.recv(4))[0]
            request_data = client_socket.recv(data_len)
            request = json.loads(request_data.decode('utf-8'))
            
            with self.lock:
                # 检查是否有空闲描述符可用
                if not self.idle_descriptors:
                    print(f"[宿主机SHM分配警告] 没有空闲描述符！")
                    print(f"[宿主机SHM分配警告] 忙碌描述符数量: {len(self.busy_descriptors)}")
                    print(f"[宿主机SHM分配警告] 依赖计数状态: {self.dependency_count}")
                    
                    # 尝试清理依赖计数为0的遗留描述符
                    print(f"[宿主机SHM分配警告] 开始清理依赖计数为0的遗留描述符...")
                    cleaned_count = 0
                    to_remove = []
                    for pid, count in list(self.dependency_count.items()):
                        if count <= 0 and pid in self.busy_descriptors:
                            desc_index, _ = self.busy_descriptors[pid]
                            del self.busy_descriptors[pid]
                            self.idle_descriptors.add(desc_index)
                            to_remove.append(pid)
                            cleaned_count += 1
                            print(f"[宿主机SHM分配警告] 清理遗留描述符: packet_id={pid}, desc_index={desc_index}")
                    
                    # 清理依赖计数字典
                    for pid in to_remove:
                        del self.dependency_count[pid]
                    
                    print(f"[宿主机SHM分配警告] 清理完成，回收了 {cleaned_count} 个描述符")
                    print(f"[宿主机SHM分配警告] 清理后空闲描述符数量: {len(self.idle_descriptors)}")
                    
                    # 如果还是没有空闲描述符，返回错误
                    if not self.idle_descriptors:
                        print(f"[宿主机SHM分配错误] 清理后仍然没有空闲描述符，分配失败！")
                        error_response = json.dumps({'error': 'no_free_descriptors'}).encode('utf-8')
                        client_socket.sendall(struct.pack('!I', len(error_response)))
                        client_socket.sendall(error_response)
                        return
                
                if self.idle_descriptors:
                    # 分配空闲描述符
                    desc_index = self.idle_descriptors.pop()
                    packet_id = self.next_packet_id
                    self.next_packet_id += 1
                    
                    print(f"[宿主机SHM分配调试] 成功分配描述符: desc_index={desc_index}, packet_id={packet_id}")
                    print(f"[宿主机SHM分配调试] 剩余空闲描述符: {len(self.idle_descriptors)}")
                    
                    # 计算数据偏移
                    data_offset = self.data_offset + (desc_index * MAX_PACKET_SIZE)
                    
                    # 创建描述符
                    descriptor = PacketDescriptor(
                        packet_id=packet_id,
                        data_offset=data_offset,
                        data_size=request.get('data_size', 0),
                        function_id=request.get('function_id', 0),
                        timestamp=int(time.time() * 1000000),
                        flags=PacketFlags.VALID
                    )
                    
                    # 添加到busy描述符
                    self.busy_descriptors[packet_id] = (desc_index, descriptor)
                    
                    # 设置依赖计数
                    self.dependency_count[packet_id] = request.get('dependency_count', 1)
                    
                    # 发送描述符给容器
                    descriptor_data = {
                        'packet_id': descriptor.packet_id,
                        'data_offset': descriptor.data_offset,
                        'data_size': descriptor.data_size,
                        'function_id': descriptor.function_id,
                        'timestamp': descriptor.timestamp,
                        'flags': descriptor.flags
                    }
                    
                    response = json.dumps(descriptor_data).encode('utf-8')
                    client_socket.sendall(struct.pack('!I', len(response)))
                    client_socket.sendall(response)
                    
                    logger.info(f"分配写入描述符: packet_id={packet_id}, desc_index={desc_index}")
                else:
                    # 发送错误响应
                    error_response = json.dumps({'error': 'no_idle_descriptors'}).encode('utf-8')
                    client_socket.sendall(struct.pack('!I', len(error_response)))
                    client_socket.sendall(error_response)
                    
        except Exception as e:
            logger.error(f"处理写入请求失败: {e}")
    
    def _handle_read_complete(self, client_socket):
        """处理读取完成通知"""
        import json
        import struct
        
        try:
            # 读取通知数据
            data_len = struct.unpack('!I', client_socket.recv(4))[0]
            notification_data = client_socket.recv(data_len)
            notification = json.loads(notification_data.decode('utf-8'))
            
            packet_id = notification['packet_id']
            
            with self.lock:
                print(f"[宿主机SHM清理调试] 收到读取完成通知: packet_id={packet_id}")
                print(f"[宿主机SHM清理调试] 当前依赖计数状态: {self.dependency_count}")
                print(f"[宿主机SHM清理调试] 当前忙碌描述符: {list(self.busy_descriptors.keys())}")
                print(f"[宿主机SHM清理调试] 当前空闲描述符数量: {len(self.idle_descriptors)}")
                
                if packet_id in self.dependency_count:
                    # 减少依赖计数
                    old_count = self.dependency_count[packet_id]
                    self.dependency_count[packet_id] -= 1
                    new_count = self.dependency_count[packet_id]
                    
                    print(f"[宿主机SHM清理调试] 依赖计数更新: packet_id={packet_id}, {old_count} -> {new_count}")
                    
                    if self.dependency_count[packet_id] <= 0:
                        # 依赖计数为0，回收描述符
                        print(f"[宿主机SHM清理调试] 依赖计数为0，开始回收描述符: packet_id={packet_id}")
                        
                        if packet_id in self.busy_descriptors:
                            desc_index, descriptor = self.busy_descriptors[packet_id]
                            del self.busy_descriptors[packet_id]
                            del self.dependency_count[packet_id]
                            self.idle_descriptors.add(desc_index)
                            
                            print(f"[宿主机SHM清理调试] ✓ 描述符回收成功: packet_id={packet_id}, desc_index={desc_index}")
                            print(f"[宿主机SHM清理调试] 回收后 - 忙碌描述符: {list(self.busy_descriptors.keys())}")
                            print(f"[宿主机SHM清理调试] 回收后 - 空闲描述符数量: {len(self.idle_descriptors)}")
                            print(f"[宿主机SHM清理调试] 回收后 - 依赖计数: {self.dependency_count}")
                            
                            logger.info(f"回收描述符: packet_id={packet_id}, desc_index={desc_index}")
                        else:
                            print(f"[宿主机SHM清理调试] ✗ 警告: packet_id={packet_id} 不在忙碌描述符中")
                    else:
                        print(f"[宿主机SHM清理调试] 依赖计数未归零，继续等待: packet_id={packet_id}, count={new_count}")
                else:
                    print(f"[宿主机SHM清理调试] ✗ 警告: packet_id={packet_id} 不在依赖计数中")
                
                # 发送确认（移到if/else块外，确保总是发送）
                ack_response = json.dumps({'status': 'ok'}).encode('utf-8')
                client_socket.sendall(struct.pack('!I', len(ack_response)))
                client_socket.sendall(ack_response)
                    
        except Exception as e:
            logger.error(f"处理读取完成通知失败: {e}")
    
    def _handle_write_complete(self, client_socket):
        """处理写入完成通知"""
        import json
        import struct
        
        try:
            # 读取通知数据
            data_len = struct.unpack('!I', client_socket.recv(4))[0]
            notification_data = client_socket.recv(data_len)
            notification = json.loads(notification_data.decode('utf-8'))
            
            packet_id = notification['packet_id']
            dependency_count = notification.get('dependency_count', 1)
            
            with self.lock:
                if packet_id in self.busy_descriptors:
                    # 更新依赖计数
                    self.dependency_count[packet_id] = dependency_count
                    
                    logger.info(f"更新依赖计数: packet_id={packet_id}, count={dependency_count}")
                    
                    # 发送确认
                    ack_response = json.dumps({'status': 'ok'}).encode('utf-8')
                    client_socket.sendall(struct.pack('!I', len(ack_response)))
                    client_socket.sendall(ack_response)
                    
        except Exception as e:
            logger.error(f"处理写入完成通知失败: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            return {
                'initialized': self._initialized,
                'shm_size': self.shm_size,
                'idle_descriptors': len(self.idle_descriptors),
                'busy_descriptors': len(self.busy_descriptors),
                'next_packet_id': self.next_packet_id,
                'data_offset': self.data_offset,
                'data_size': self.header.data_size if self.header else 0,
                'is_creator': self._is_creator
            }
 
    def _cleanup(self):
        """清理资源"""
        # 停止Socket服务器
        if self.socket_server:
            try:
                self.socket_server.close()
                # 删除socket文件
                import os
                if os.path.exists('/tmp/faasflow_host_shm.sock'):
                    os.unlink('/tmp/faasflow_host_shm.sock')
            except Exception as e:
                logger.warning(f"清理Socket服务器时出现警告: {e}")
            finally:
                self.socket_server = None
        
        # 清理共享内存
        if self.shared_memory:
            try:
                # 先关闭共享内存
                self.shared_memory.close()
                
                # 如果是创建者，则删除共享内存
                if self._is_creator:
                    try:
                        self.shared_memory.unlink()
                    except FileNotFoundError:
                        # 共享内存可能已经被删除
                        pass
                        
            except Exception as e:
                # 忽略清理时的警告，这是正常的
                if "cannot close exported pointers exist" not in str(e):
                    logger.warning(f"清理共享内存时出现警告: {e}")
            finally:
                self.shared_memory = None
        
        self._initialized = False
    
    def cleanup(self):
        """清理共享内存资源"""
        if not self._initialized:
            return
        
        try:
            with self.lock:
                self._cleanup()
                logger.info("宿主机共享内存清理完成")
        except Exception as e:
            logger.error(f"清理共享内存时发生异常: {e}")
    
    def __enter__(self):
        """上下文管理器入口"""
        if not self.init_shm_pool():
            raise RuntimeError("宿主机共享内存池初始化失败")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.cleanup()


# 全局共享内存管理器实例（可选）
_global_shm_manager = None

def get_global_shm_manager() -> HostSharedMemoryManager:
    """获取全局共享内存管理器实例"""
    global _global_shm_manager
    if _global_shm_manager is None:
        _global_shm_manager = HostSharedMemoryManager()
        _global_shm_manager.init_shm_pool()
    return _global_shm_manager


# 测试代码
if __name__ == "__main__":
    print("=== 宿主机共享内存管理器测试 ===")
    
    try:
        with HostSharedMemoryManager() as shm:
            print("1. 初始化测试: 成功")
            
            # 获取统计信息
            stats = shm.get_stats()
            print(f"2. 统计信息: {stats}")
            
            # 测试描述符分配（模拟容器请求）
            print("3. 模拟容器请求测试...")
            
            # 这里可以添加更多的测试逻辑
            # 比如模拟容器请求读取/写入描述符
            
    except Exception as e:
        print(f"测试失败: {e}")
    
    print("=== 测试完成 ===")
