#!/usr/bin/env python3
"""
FaaSFlow共享内存工具函数
提供共享内存相关的工具函数和协议定义
"""

import json
import logging
import time
import socket
import threading
import struct
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
# 根据配置选择SHM版本
try:
    from config import config
    USE_OPTIMIZED_SHM = getattr(config, 'USE_OPTIMIZED_SHM', False)
except ImportError:
    USE_OPTIMIZED_SHM = False
    print(f"USE_OPTIMIZED_SHM: {USE_OPTIMIZED_SHM}")
if USE_OPTIMIZED_SHM:
    from optimized_shm import OptimizedHostSharedMemoryManager
    HostSharedMemoryManager = OptimizedHostSharedMemoryManager
    get_global_shm_manager = None  # 优化版不需要全局管理器
    MAX_PACKET_SIZE = getattr(config, 'OPTIMIZED_MAX_PACKET_SIZE', 5 * 1024 * 1024)
else:
    from multiprocessing_shm import HostSharedMemoryManager, get_global_shm_manager, MAX_PACKET_SIZE

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 共享内存配置
SHM_NAME = 'faasflow_shm'
SHM_SIZE = 100 * 1024 * 1024  # 100MB
SHM_MAGIC = 0x12345678

# 通信协议常量
PROTOCOL_VERSION = 1
PACKET_TYPE_INPUT = 1
PACKET_TYPE_OUTPUT = 2
PACKET_TYPE_METADATA = 3

# Socket请求类型
REQUEST_TYPE_READ_DESCRIPTOR = 1
REQUEST_TYPE_WRITE_DESCRIPTOR = 2
REQUEST_TYPE_READ_COMPLETE = 3
REQUEST_TYPE_WRITE_COMPLETE = 4

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
    """FaaSFlow专用的共享内存管理器（宿主机版本）"""
    
    def __init__(self, shm_name: str = None, shm_size: int = None):
        # 根据配置选择SHM参数
        if USE_OPTIMIZED_SHM:
            self.shm_name = shm_name or getattr(config, 'OPTIMIZED_SHM_NAME', 'faasflow_shm_opt')
            self.shm_size = shm_size or getattr(config, 'OPTIMIZED_SHM_SIZE', 5 * 1024 * 1024 * 1024)
        else:
            self.shm_name = shm_name or getattr(config, 'SHM_NAME', SHM_NAME)
            self.shm_size = shm_size or getattr(config, 'SHM_SIZE', SHM_SIZE)
        
        self.shm_manager = None
        self.initialized = False
        
    def init_shm_pool(self) -> bool:
        """初始化共享内存池（宿主机版本）"""
        try:
            shm_type = "优化版" if USE_OPTIMIZED_SHM else "旧版"
            logger.info(f"初始化FaaSFlow宿主机共享内存池({shm_type}): {self.shm_name}, 大小: {self.shm_size}")
            
            if USE_OPTIMIZED_SHM:
                # 使用优化版SHM
                self.shm_manager = OptimizedHostSharedMemoryManager(shm_name=self.shm_name, shm_size=self.shm_size)
            else:
                # 使用旧版SHM
                self.shm_manager = HostSharedMemoryManager(shm_name=self.shm_name, shm_size=self.shm_size)
            
            if not self.shm_manager.init_shm_pool():
                logger.error(f"宿主机共享内存池初始化失败({shm_type})")
                return False
            
            self.initialized = True
            logger.info(f"FaaSFlow宿主机共享内存池初始化成功({shm_type})")
            return True
            
        except Exception as e:
            logger.error(f"初始化宿主机共享内存池时发生异常: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """获取共享内存统计信息"""
        if not self.initialized or not self.shm_manager:
            return {'initialized': False}
        
        return self.shm_manager.get_stats()
    
    def start_container_request_handler(self):
        """启动容器请求处理器"""
        if not self.initialized:
            logger.error("共享内存管理器未初始化，无法启动容器请求处理器")
            return False
        
        if USE_OPTIMIZED_SHM:
            # 优化版SHM不需要Socket服务器
            logger.info("FaaSFlow容器请求处理器已就绪（优化版SHM，无需Socket服务器）")
        else:
            # 旧版SHM需要Socket服务器
            logger.info("FaaSFlow容器请求处理器已就绪（旧版SHM，使用Socket服务器）")
        
        return True
    
    
    def cleanup(self):
        """清理共享内存资源"""
        # 清理共享内存管理器（Socket服务器清理由HostSharedMemoryManager负责）
        if self.shm_manager:
            try:
                self.shm_manager.cleanup()
                logger.info("FaaSFlow宿主机共享内存管理器清理完成")
            except Exception as e:
                logger.error(f"清理宿主机共享内存管理器时发生异常: {e}")
        
        self.initialized = False


# 全局FaaSFlow共享内存管理器实例
_global_faasflow_shm_manager = None

def get_global_faasflow_shm_manager() -> FaaSFlowShmManager:
    """获取全局FaaSFlow共享内存管理器实例"""
    global _global_faasflow_shm_manager
    if _global_faasflow_shm_manager is None:
        _global_faasflow_shm_manager = FaaSFlowShmManager()
        # 注意：不在这里初始化，由workersp.py统一初始化
    return _global_faasflow_shm_manager


def is_shm_enabled() -> bool:
    """检查是否启用共享内存模式"""
    try:
        from config import config
        print(f"检查SHM配置: hasattr={hasattr(config, 'ENABLE_SHARED_MEMORY')}")
        if hasattr(config, 'ENABLE_SHARED_MEMORY'):
            result = config.ENABLE_SHARED_MEMORY
            print(f"SHM配置值: {result}")
            return result
        else:
            print("SHM配置属性不存在")
            return False
    except ImportError as e:
        print(f"导入config失败: {e}")
        return False

def get_shm_config() -> Dict[str, Any]:
    """获取共享内存配置"""
    try:
        import config
        return {
            'shm_name': getattr(config, 'SHM_NAME', SHM_NAME),
            'shm_size': getattr(config, 'SHM_SIZE', SHM_SIZE),
            'shm_magic': getattr(config, 'SHM_MAGIC', SHM_MAGIC),
            'protocol_version': getattr(config, 'SHM_PROTOCOL_VERSION', PROTOCOL_VERSION),
            'shm_threshold_size': getattr(config, 'SHM_THRESHOLD_SIZE', 1024 * 1024),
            'shm_proxy_port': getattr(config, 'SHM_PROXY_PORT', 8081),
            'shm_container_port': getattr(config, 'SHM_CONTAINER_PORT', 8082),
            'enabled': is_shm_enabled()
        }
    except ImportError:
        return {
            'shm_name': SHM_NAME,
            'shm_size': SHM_SIZE,
            'shm_magic': SHM_MAGIC,
            'protocol_version': PROTOCOL_VERSION,
            'shm_threshold_size': 1024 * 1024,
            'shm_proxy_port': 8081,
            'shm_container_port': 8082,
            'enabled': False
        }
