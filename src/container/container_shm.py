#!/usr/bin/env python3
"""
容器端共享内存管理器
负责根据描述符读取和写入共享内存数据
"""

import multiprocessing.shared_memory as shm
import threading
import time
import logging
import struct
import json
import socket
from typing import Optional, Tuple, Dict, Any
from dataclasses import dataclass

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 配置常量
SHM_NAME = 'faasflow_shm'
SHM_SIZE = 500 * 1024 * 1024  # 500MB
MAX_PACKET_SIZE = 1 * 1024 * 1024  # 1MB
SHM_MAGIC = 0x12345678

# Socket请求类型
REQUEST_TYPE_READ_DESCRIPTOR = 1
REQUEST_TYPE_WRITE_DESCRIPTOR = 2
REQUEST_TYPE_READ_COMPLETE = 3
REQUEST_TYPE_WRITE_COMPLETE = 4

# Socket配置
SOCKET_PATH = '/tmp/faasflow_host_shm.sock'

@dataclass
class PacketDescriptor:
    """包描述符结构"""
    packet_id: int = 0
    data_offset: int = 0
    data_size: int = 0
    function_id: int = 0
    timestamp: int = 0
    flags: int = 0

class ContainerSharedMemoryManager:
    """
    容器端共享内存管理器
    负责根据描述符读取和写入共享内存数据
    """
    
    def __init__(self, shm_name: str = SHM_NAME):
        """
        初始化容器端共享内存管理器
        
        Args:
            shm_name: 共享内存名称
        """
        self.shm_name = shm_name
        self.shared_memory = None
        self.lock = threading.RLock()
        self._initialized = False
        
        logger.info(f"容器端共享内存管理器初始化: {shm_name}")
    
    def init_shm_connection(self) -> bool:
        """
        连接到宿主机共享内存池
        
        Returns:
            bool: 连接是否成功
        """
        if self._initialized:
            logger.warning("共享内存连接已经建立")
            return True
        
        try:
            with self.lock:
                # 连接到现有的共享内存
                self.shared_memory = shm.SharedMemory(name=self.shm_name)
                logger.info("连接到宿主机共享内存池")
                
                self._initialized = True
                logger.info("容器端共享内存连接成功")
                return True
                
        except Exception as e:
            logger.error(f"连接共享内存池时发生异常: {e}")
            self._cleanup()
            return False
    def request_read_descriptor(self, packet_id: int) -> Optional[Dict[str, Any]]:
        """
        向宿主机请求读取描述符
        
        Args:
            packet_id: 包ID
            
        Returns:
            Optional[Dict[str, Any]]: 描述符信息或None
        """
        try:
            # 连接到宿主机Socket
            client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client_socket.connect(SOCKET_PATH)
            
            # 发送请求类型
            client_socket.sendall(struct.pack('!I', REQUEST_TYPE_READ_DESCRIPTOR))
            
            # 发送请求数据
            request_data = json.dumps({'packet_id': packet_id}).encode('utf-8')
            client_socket.sendall(struct.pack('!I', len(request_data)))
            client_socket.sendall(request_data)
            
            # 接收响应
            response_len = struct.unpack('!I', client_socket.recv(4))[0]
            response_data = client_socket.recv(response_len)
            response = json.loads(response_data.decode('utf-8'))
            
            client_socket.close()
            
            if 'error' in response:
                logger.error(f"请求读取描述符失败: {response['error']}")
                return None
            
            logger.info(f"获取读取描述符成功: packet_id={packet_id}")
            return response
            
        except Exception as e:
            logger.error(f"请求读取描述符时发生异常: {e}")
            return None
    
    def request_write_descriptor(self, data_size: int, function_id: int, dependency_count: int = 1) -> Optional[Dict[str, Any]]:
        """
        向宿主机请求写入描述符
        
        Args:
            data_size: 数据大小
            function_id: 函数ID
            dependency_count: 依赖计数
            
        Returns:
            Optional[Dict[str, Any]]: 描述符信息或None
        """
        try:
            print(f"[容器SHM调试] 开始连接Socket: {SOCKET_PATH}")
            # 连接到宿主机Socket
            client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client_socket.connect(SOCKET_PATH)
            print(f"[容器SHM调试] Socket连接成功")
            
            # 发送请求类型
            print(f"[容器SHM调试] 发送请求类型: {REQUEST_TYPE_WRITE_DESCRIPTOR}")
            client_socket.sendall(struct.pack('!I', REQUEST_TYPE_WRITE_DESCRIPTOR))
            
            # 发送请求数据
            request_data = json.dumps({
                'data_size': data_size,
                'function_id': function_id,
                'dependency_count': dependency_count
            }).encode('utf-8')
            print(f"[容器SHM调试] 发送请求数据: {request_data}")
            client_socket.sendall(struct.pack('!I', len(request_data)))
            client_socket.sendall(request_data)
            print(f"[容器SHM调试] 请求数据发送完成")
            
            # 接收响应
            print(f"[容器SHM调试] 开始接收响应")
            response_len = struct.unpack('!I', client_socket.recv(4))[0]
            print(f"[容器SHM调试] 响应长度: {response_len}")
            response_data = client_socket.recv(response_len)
            print(f"[容器SHM调试] 响应数据: {response_data}")
            response = json.loads(response_data.decode('utf-8'))
            print(f"[容器SHM调试] 解析响应: {response}")
            
            client_socket.close()
            print(f"[容器SHM调试] Socket连接已关闭")
            
            if 'error' in response:
                logger.error(f"请求写入描述符失败: {response['error']}")
                return None
            
            logger.info(f"获取写入描述符成功: packet_id={response.get('packet_id')}")
            return response
            
        except Exception as e:
            logger.error(f"请求写入描述符时发生异常: {e}")
            return None
    
    def notify_read_complete(self, packet_id: int) -> bool:
        """
        通知宿主机读取完成
        
        Args:
            packet_id: 包ID
            
        Returns:
            bool: 通知是否成功
        """
        try:
            # 连接到宿主机Socket
            client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client_socket.connect(SOCKET_PATH)
            
            # 发送请求类型
            client_socket.sendall(struct.pack('!I', REQUEST_TYPE_READ_COMPLETE))
            
            # 发送通知数据
            notification_data = json.dumps({'packet_id': packet_id}).encode('utf-8')
            client_socket.sendall(struct.pack('!I', len(notification_data)))
            client_socket.sendall(notification_data)
            
            # 接收确认
            response_len = struct.unpack('!I', client_socket.recv(4))[0]
            response_data = client_socket.recv(response_len)
            response = json.loads(response_data.decode('utf-8'))
            
            client_socket.close()
            
            if response.get('status') == 'ok':
                logger.info(f"读取完成通知成功: packet_id={packet_id}")
                return True
            else:
                logger.error(f"读取完成通知失败: {response}")
                return False
            
        except Exception as e:
            logger.error(f"通知读取完成时发生异常: {e}")
            return False
    
    def notify_write_complete(self, packet_id: int, dependency_count: int) -> bool:
        """
        通知宿主机写入完成
        
        Args:
            packet_id: 包ID
            dependency_count: 依赖计数
            
        Returns:
            bool: 通知是否成功
        """
        try:
            # 连接到宿主机Socket
            client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client_socket.connect(SOCKET_PATH)
            
            # 发送请求类型
            client_socket.sendall(struct.pack('!I', REQUEST_TYPE_WRITE_COMPLETE))
            
            # 发送通知数据
            notification_data = json.dumps({
                'packet_id': packet_id,
                'dependency_count': dependency_count
            }).encode('utf-8')
            client_socket.sendall(struct.pack('!I', len(notification_data)))
            client_socket.sendall(notification_data)
            
            # 接收确认
            response_len = struct.unpack('!I', client_socket.recv(4))[0]
            response_data = client_socket.recv(response_len)
            response = json.loads(response_data.decode('utf-8'))
            
            client_socket.close()
            
            if response.get('status') == 'ok':
                logger.info(f"写入完成通知成功: packet_id={packet_id}")
                return True
            else:
                logger.error(f"写入完成通知失败: {response}")
                return False
            
        except Exception as e:
            logger.error(f"通知写入完成时发生异常: {e}")
            return False
    
    def read_data(self, descriptor: Dict[str, Any]) -> Optional[bytes]:
        """
        根据描述符读取共享内存数据
        
        Args:
            descriptor: 包描述符信息
            
        Returns:
            Optional[bytes]: 数据或None
        """
        if not self._initialized:
            logger.error("共享内存连接未建立")
            return None
        
        try:
            with self.lock:
                data_offset = descriptor['data_offset']
                data_size = descriptor['data_size']
                
                # 检查数据偏移量是否有效
                if data_offset < 0 or data_size <= 0:
                    logger.error(f"数据偏移量无效: offset={data_offset}, size={data_size}")
                    return None
                
                # 检查数据区域边界
                if data_offset + data_size > len(self.shared_memory.buf):
                    logger.error(f"数据超出共享内存边界: offset={data_offset}, size={data_size}")
                    return None
                
                # 读取数据
                data = bytes(self.shared_memory.buf[data_offset:data_offset + data_size])
                logger.info(f"读取数据成功: packet_id={descriptor['packet_id']}, 大小={len(data)}")
                return data
                
        except Exception as e:
            logger.error(f"读取数据时发生异常: {e}")
            return None
    
    def write_data(self, descriptor: Dict[str, Any], data: bytes) -> bool:
        """
        根据描述符写入共享内存数据
        
        Args:
            descriptor: 包描述符信息
            data: 要写入的数据
            
        Returns:
            bool: 写入是否成功
        """
        if not self._initialized:
            logger.error("共享内存连接未建立")
            return False
        
        try:
            with self.lock:
                data_offset = descriptor['data_offset']
                data_size = descriptor['data_size']
                
                # 检查数据大小是否匹配
                if len(data) > data_size:
                    logger.error(f"数据大小超出描述符限制: {len(data)} > {data_size}")
                    return False
                
                # 检查数据偏移量是否有效
                if data_offset < 0 or data_size <= 0:
                    logger.error(f"数据偏移量无效: offset={data_offset}, size={data_size}")
                    return False
                
                # 检查数据区域边界
                if data_offset + data_size > len(self.shared_memory.buf):
                    logger.error(f"数据超出共享内存边界: offset={data_offset}, size={data_size}")
                    return False
                
                # 写入数据
                self.shared_memory.buf[data_offset:data_offset + len(data)] = data
                logger.info(f"写入数据成功: packet_id={descriptor['packet_id']}, 大小={len(data)}")
                return True
                
        except Exception as e:
            logger.error(f"写入数据时发生异常: {e}")
            return False
    
    def get_data_view(self, descriptor: Dict[str, Any]) -> Optional[memoryview]:
        """
        根据描述符获取数据内存视图
        
        Args:
            descriptor: 包描述符信息
            
        Returns:
            Optional[memoryview]: 内存视图或None
        """
        if not self._initialized:
            logger.error("共享内存连接未建立")
            return None
        
        try:
            with self.lock:
                data_offset = descriptor['data_offset']
                data_size = descriptor['data_size']
                
                # 检查数据偏移量是否有效
                if data_offset < 0 or data_size <= 0:
                    logger.error(f"数据偏移量无效: offset={data_offset}, size={data_size}")
                    return None
                
                # 检查数据区域边界
                if data_offset + data_size > len(self.shared_memory.buf):
                    logger.error(f"数据超出共享内存边界: offset={data_offset}, size={data_size}")
                    return None
                
                # 创建内存视图
                data_view = memoryview(self.shared_memory.buf)[data_offset:data_offset + data_size]
                logger.info(f"获取数据视图成功: packet_id={descriptor['packet_id']}, 大小={data_size}")
                return data_view
                
        except Exception as e:
            logger.error(f"获取数据视图时发生异常: {e}")
            return None
    
    def _cleanup(self):
        """清理资源"""
        if self.shared_memory:
            try:
                self.shared_memory.close()
            except Exception as e:
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
                logger.info("容器端共享内存清理完成")
        except Exception as e:
            logger.error(f"清理共享内存时发生异常: {e}")
    
    def __enter__(self):
        """上下文管理器入口"""
        if not self.init_shm_connection():
            raise RuntimeError("容器端共享内存连接失败")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.cleanup()


# 便捷函数
def read_data_by_packet_id(packet_id: int, shm_manager: ContainerSharedMemoryManager) -> Optional[bytes]:
    """
    根据包ID读取数据的便捷函数
    
    Args:
        packet_id: 包ID
        shm_manager: 容器共享内存管理器
        
    Returns:
        Optional[bytes]: 数据或None
    """
    try:
        # 请求读取描述符
        descriptor = shm_manager.request_read_descriptor(packet_id)
        if not descriptor:
            return None
        
        # 读取数据
        data = shm_manager.read_data(descriptor)
        
        # 通知读取完成
        shm_manager.notify_read_complete(packet_id)
        
        return data
    except Exception as e:
        logger.error(f"根据包ID读取数据失败: {e}")
        return None


def write_data_by_packet_id(data: bytes, function_id: int, shm_manager: ContainerSharedMemoryManager, dependency_count: int = 1) -> Optional[int]:
    """
    写入数据并返回包ID的便捷函数
    
    Args:
        data: 要写入的数据
        function_id: 函数ID
        shm_manager: 容器共享内存管理器
        dependency_count: 依赖计数
        
    Returns:
        Optional[int]: 包ID或None
    """
    try:
        # 请求写入描述符
        descriptor = shm_manager.request_write_descriptor(len(data), function_id, dependency_count)
        if not descriptor:
            return None
        
        # 写入数据
        if not shm_manager.write_data(descriptor, data):
            return None
        
        # 通知写入完成
        shm_manager.notify_write_complete(descriptor['packet_id'], dependency_count)
        
        return descriptor['packet_id']
    except Exception as e:
        logger.error(f"写入数据失败: {e}")
        return None


# 全局容器共享内存管理器实例
_global_container_shm_manager = None

def get_global_container_shm_manager() -> ContainerSharedMemoryManager:
    """获取全局容器共享内存管理器实例"""
    global _global_container_shm_manager
    if _global_container_shm_manager is None:
        _global_container_shm_manager = ContainerSharedMemoryManager()
        _global_container_shm_manager.init_shm_connection()
    return _global_container_shm_manager


# 测试代码
if __name__ == "__main__":
    print("=== 容器端共享内存管理器测试 ===")
    
    try:
        with ContainerSharedMemoryManager() as shm:
            print("1. 连接测试: 成功")
            
            # 测试写入数据
            test_data = b"Hello, Container Shared Memory!"
            packet_id = write_data_by_packet_id(test_data, 1, shm, dependency_count=2)
            if packet_id:
                print(f"2. 写入数据测试: 成功, 包ID={packet_id}")
                
                # 测试读取数据
                read_data = read_data_by_packet_id(packet_id, shm)
                if read_data == test_data:
                    print("3. 读取数据测试: 成功")
                else:
                    print("3. 读取数据测试: 失败")
            else:
                print("2. 写入数据测试: 失败")
            
    except Exception as e:
        print(f"测试失败: {e}")
    
    print("=== 测试完成 ===")
