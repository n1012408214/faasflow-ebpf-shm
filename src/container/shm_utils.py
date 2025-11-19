#!/usr/bin/env python3
"""
容器端FaaSFlow共享内存工具函数
提供容器端共享内存相关的工具函数和协议定义
"""

import json
import logging
import time
import socket
import struct
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from container_shm import ContainerSharedMemoryManager, get_global_container_shm_manager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 共享内存配置
SHM_NAME = 'faasflow_shm'
SHM_SIZE = 500 * 1024 * 1024  # 500MB
SHM_MAGIC = 0x12345678

# 通信协议常量
PROTOCOL_VERSION = 1
PACKET_TYPE_INPUT = 1
PACKET_TYPE_OUTPUT = 2
PACKET_TYPE_METADATA = 3

# Socket配置
SOCKET_PATH = '/tmp/faasflow_host_shm.sock'

@dataclass
class ShmPacketDescriptor:
    """共享内存包描述符"""
    packet_id: int
    function_id: int
    data_size: int
    packet_type: int
    timestamp: int
    request_id: str
    workflow_name: str
    template_name: str
    block_name: str
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'packet_id': self.packet_id,
            'function_id': self.function_id,
            'data_size': self.data_size,
            'packet_type': self.packet_type,
            'timestamp': self.timestamp,
            'request_id': self.request_id,
            'workflow_name': self.workflow_name,
            'template_name': self.template_name,
            'block_name': self.block_name
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ShmPacketDescriptor':
        """从字典创建"""
        return cls(
            packet_id=data['packet_id'],
            function_id=data['function_id'],
            data_size=data['data_size'],
            packet_type=data['packet_type'],
            timestamp=data['timestamp'],
            request_id=data['request_id'],
            workflow_name=data['workflow_name'],
            template_name=data['template_name'],
            block_name=data['block_name']
        )

class FaaSFlowShmManager:
    """容器端FaaSFlow专用的共享内存管理器"""
    
    def __init__(self, shm_name: str = SHM_NAME):
        self.shm_name = shm_name
        self.shm_manager = None
        self.initialized = False
        
    def init_shm_connection(self) -> bool:
        """连接到宿主机共享内存池"""
        try:
            logger.info(f"初始化容器端FaaSFlow共享内存连接: {self.shm_name}")
            
            self.shm_manager = ContainerSharedMemoryManager(shm_name=self.shm_name)
            if not self.shm_manager.init_shm_connection():
                logger.error("容器端共享内存连接失败")
                return False
            
            self.initialized = True
            logger.info("容器端FaaSFlow共享内存连接成功")
            return True
            
        except Exception as e:
            logger.error(f"初始化容器端共享内存连接时发生异常: {e}")
            return False
    
    def store_workflow_data(self, request_id: str, workflow_name: str, template_name: str, 
                           block_name: str, function_id: int, data: bytes, 
                           packet_type: int = PACKET_TYPE_INPUT, dependency_count: int = 1) -> Optional[int]:
        """存储工作流数据到共享内存"""
        total_start_time = time.time()
        
        if not self.initialized:
            logger.error("共享内存管理器未初始化")
            return None
        
        try:
            # 确保data是bytes类型
            prepare_start = time.time()
            if isinstance(data, str):
                data_bytes = data.encode('utf-8')
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                data_bytes = str(data).encode('utf-8')
            prepare_end = time.time()
            
            # 使用容器端管理器写入数据
            request_desc_start = time.time()
            descriptor = self.shm_manager.request_write_descriptor(
                data_size=len(data_bytes),
                function_id=function_id,
                dependency_count=dependency_count
            )
            request_desc_end = time.time()
            
            if not descriptor:
                logger.error("获取写入描述符失败")
                return None
            
            # 写入数据
            write_start = time.time()
            if not self.shm_manager.write_data(descriptor, data_bytes):
                logger.error("写入数据到共享内存失败")
                return None
            write_end = time.time()
            
            # 通知写入完成
            notify_start = time.time()
            if not self.shm_manager.notify_write_complete(descriptor['packet_id'], dependency_count):
                logger.error("通知写入完成失败")
                return None
            notify_end = time.time()
            
            total_time = time.time() - total_start_time
            prepare_time = prepare_end - prepare_start
            request_time = request_desc_end - request_desc_start
            write_time = write_end - write_start
            notify_time = notify_end - notify_start
            
            print(f"[SHM写入阶段] packet_id={descriptor['packet_id']}, "
                  f"准备={prepare_time*1000:.2f}ms, 请求描述符={request_time*1000:.2f}ms, "
                  f"写入={write_time*1000:.2f}ms, 通知={notify_time*1000:.2f}ms, "
                  f"总计={total_time*1000:.2f}ms")
            
            return descriptor['packet_id']
                
        except Exception as e:
            logger.error(f"存储工作流数据时发生异常: {e}")
            return None
    
    def retrieve_workflow_data(self, packet_id: int) -> Optional[Tuple[ShmPacketDescriptor, bytes]]:
        """从共享内存检索工作流数据"""
        if not self.initialized:
            logger.error("共享内存管理器未初始化")
            return None
        
        try:
            # 请求读取描述符
            descriptor = self.shm_manager.request_read_descriptor(packet_id)
            if not descriptor:
                logger.error(f"获取读取描述符失败: 包ID={packet_id}")
                return None
            
            # 读取数据
            data = self.shm_manager.read_data(descriptor)
            if not data:
                logger.error(f"从共享内存读取数据失败: 包ID={packet_id}")
                return None
            
            # 通知读取完成
            self.shm_manager.notify_read_complete(packet_id)
            
            # 创建描述符对象
            shm_descriptor = ShmPacketDescriptor(
                packet_id=descriptor['packet_id'],
                function_id=descriptor['function_id'],
                data_size=descriptor['data_size'],
                packet_type=PACKET_TYPE_INPUT,  # 默认类型
                timestamp=descriptor['timestamp'],
                request_id="",  # 容器端无法获取这些信息
                workflow_name="",
                template_name="",
                block_name=""
            )
            
            logger.info(f"从共享内存读取数据成功: 包ID={packet_id}, 大小={len(data)}")
            return shm_descriptor, data
            
        except Exception as e:
            logger.error(f"检索工作流数据时发生异常: {e}")
            return None
    
    def cleanup(self):
        """清理共享内存资源"""
        if self.shm_manager:
            try:
                self.shm_manager.cleanup()
                logger.info("容器端FaaSFlow共享内存管理器清理完成")
            except Exception as e:
                logger.error(f"清理容器端共享内存管理器时发生异常: {e}")
        
        self.initialized = False
    
    def post_data(self, key: str, val, datatype: str = 'json', function_id: int = 1, dependency_count: int = 1) -> Optional[Dict[str, Any]]:
        """
        适配store.py中_post_to_shm方法的数据输出
        
        Args:
            key: 数据键名
            val: 数据值
            datatype: 数据类型 ('json' 或 'octet')
            function_id: 函数ID
            dependency_count: 依赖计数
            
        Returns:
            Optional[Dict[str, Any]]: 包含包描述符的字典或None
        """
        if not self.initialized:
            logger.error("共享内存管理器未初始化")
            return None
        
        try:
            # 序列化数据
            if datatype == 'json':
                if isinstance(val, str):
                    data_bytes = val.encode('utf-8')
                else:
                    data_bytes = json.dumps(val).encode('utf-8')
            elif datatype == 'octet':
                if isinstance(val, bytes):
                    data_bytes = val
                else:
                    data_bytes = str(val).encode('utf-8')
            else:
                data_bytes = str(val).encode('utf-8')
            
            # 存储到共享内存
            packet_id = self.store_workflow_data(
                request_id="",  # 容器端无法获取这些信息
                workflow_name="",
                template_name="",
                block_name="",
                function_id=function_id,
                data=data_bytes,
                dependency_count=dependency_count
            )
            
            if not packet_id:
                logger.error("存储数据到共享内存失败")
                return None
            
            # 创建适配store.py的描述符格式
            descriptor = {
                'packet_id': packet_id,
                'key': key,
                'datatype': datatype,
                'size': len(data_bytes),
                'function_id': function_id,
                'timestamp': int(time.time() * 1000000)
            }
            
            logger.info(f"共享内存数据输出: {key}, 包ID={packet_id}, 大小={len(data_bytes)} bytes")
            return descriptor
            
        except Exception as e:
            logger.error(f"共享内存数据输出失败: {e}")
            return None
    
    def fetch_data(self, packet_id: int) -> Optional[bytes]:
        """
        适配store.py中fetch_from_shm方法的数据读取
        
        Args:
            packet_id: 包ID
            
        Returns:
            Optional[bytes]: 数据或None
        """
        if not self.initialized:
            logger.error("共享内存管理器未初始化")
            return None
        
        try:
            # 从共享内存读取数据
            result = self.retrieve_workflow_data(packet_id)
            if result:
                descriptor, data = result
                logger.info(f"共享内存数据读取成功: 包ID={packet_id}, 大小={len(data)} bytes")
                return data
            else:
                logger.error(f"共享内存数据读取失败: 包ID={packet_id}")
                return None
                
        except Exception as e:
            logger.error(f"从共享内存读取数据失败: {e}")
            return None

# 全局容器端FaaSFlow共享内存管理器实例
_global_faasflow_shm_manager = None

def get_global_faasflow_shm_manager() -> FaaSFlowShmManager:
    """获取全局容器端FaaSFlow共享内存管理器实例"""
    global _global_faasflow_shm_manager
    if _global_faasflow_shm_manager is None:
        _global_faasflow_shm_manager = FaaSFlowShmManager()
        _global_faasflow_shm_manager.init_shm_connection()
    return _global_faasflow_shm_manager

def store_workflow_data_global(request_id: str, workflow_name: str, template_name: str, 
                              block_name: str, function_id: int, data: bytes, 
                              packet_type: int = PACKET_TYPE_INPUT, dependency_count: int = 1) -> Optional[int]:
    """使用全局管理器存储工作流数据"""
    shm_manager = get_global_faasflow_shm_manager()
    return shm_manager.store_workflow_data(request_id, workflow_name, template_name, 
                                          block_name, function_id, data, packet_type, dependency_count)

def retrieve_workflow_data_global(packet_id: int) -> Optional[Tuple[ShmPacketDescriptor, bytes]]:
    """使用全局管理器检索工作流数据"""
    shm_manager = get_global_faasflow_shm_manager()
    return shm_manager.retrieve_workflow_data(packet_id)

def post_data_global(key: str, val, datatype: str = 'json', function_id: int = 1, dependency_count: int = 1) -> Optional[int]:
    """使用全局管理器输出数据（适配store.py）"""
    shm_manager = get_global_faasflow_shm_manager()
    descriptor = shm_manager.post_data(key, val, datatype, function_id, dependency_count)
    return descriptor['packet_id'] if descriptor else None

def fetch_data_global(packet_id: int) -> Optional[bytes]:
    """使用全局管理器读取数据（适配store.py）"""
    shm_manager = get_global_faasflow_shm_manager()
    return shm_manager.fetch_data(packet_id)

def is_shm_enabled() -> bool:
    """检查是否启用共享内存模式"""
    # 这里可以从配置文件读取，暂时返回True
    return True

def get_shm_config() -> Dict[str, Any]:
    """获取共享内存配置"""
    return {
        'shm_name': SHM_NAME,
        'shm_size': SHM_SIZE,
        'shm_magic': SHM_MAGIC,
        'protocol_version': PROTOCOL_VERSION,
        'socket_path': SOCKET_PATH,
        'enabled': is_shm_enabled()
    }
