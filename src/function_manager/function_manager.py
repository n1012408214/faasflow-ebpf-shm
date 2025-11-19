from typing import List, Dict, Optional
import gevent
import os
import docker
from src.function_manager.template import Template
from src.function_manager.template_info import TemplateInfo
from src.function_manager.port_manager import PortManager
from src.workflow_manager.shm_utils import FaaSFlowShmManager, is_shm_enabled, get_shm_config

dispatch_interval = 0.003
regular_clean_interval = 5.000


class FunctionManager:
    def __init__(self, min_port, config_path):
        self.templates_info = TemplateInfo.parse(config_path)
        self.port_manager = PortManager(min_port, min_port + 5000)
        self.client = docker.from_env()
        
        # 共享内存管理器（由workersp.py统一初始化）
        self.shm_manager = None
        self.shm_enabled = is_shm_enabled()
        if self.shm_enabled:
            # 使用全局SHM管理器实例
            from src.workflow_manager.shm_utils import get_global_faasflow_shm_manager
            self.shm_manager = get_global_faasflow_shm_manager()
            if self.shm_manager and self.shm_manager.initialized:
                print(f"FunctionManager共享内存模式已启用: {get_shm_config()}")
            else:
                print("警告: FunctionManager共享内存管理器不可用，将使用HTTP模式")
                self.shm_enabled = True
        
        self.templates: Dict[str, Template] = {
            template_info.template_name: Template(self.client, template_info, self.port_manager,
                                                  len(template_info.blocks), template_info.cpus, self.shm_enabled)
            for template_info in self.templates_info
        }
        self.init()

    def init(self):
        print('Clearing previous containers...')
        #os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
        gevent.spawn_later(dispatch_interval, self.dispatch_event)
        gevent.spawn_later(regular_clean_interval, self.regular_clean_event)

    def dispatch_event(self):
        gevent.spawn_later(dispatch_interval, self.dispatch_event)
        for func in self.templates.values():
            gevent.spawn(func.dispatch_request)

    def regular_clean_event(self):
        gevent.spawn_later(regular_clean_interval, self.regular_clean_event)
        for func in self.templates.values():
            gevent.spawn(func.regular_clean)

    def allocate(self, request_id, workflow_name, function_name, function_info):
        self.templates[function_name].allocate(request_id, workflow_name, function_name, function_info)

    def send_data(self, request_id, workflow_name, function_name, datas, datatype):
        self.templates[function_name].send_data(request_id, workflow_name, function_name, datas, datatype)

    def allocate_block(self, request_id, workflow_name, template_name, templates_infos, block_name, block_inputs: dict,
                       block_infos):
        assert template_name in self.templates
        self.templates[template_name].allocate_block(request_id, workflow_name, template_name, templates_infos,
                                                     block_name, block_inputs, block_infos)
    
    def allocate_block_shm(self, request_id, workflow_name, template_name, templates_infos, block_name, descriptor_data: dict,
                           block_infos):
        """使用共享内存分配块"""
        assert template_name in self.templates
        self.templates[template_name].allocate_block_shm(request_id, workflow_name, template_name, templates_infos,
                                                         block_name, descriptor_data, block_infos)

    def preempt_block(self, request_id, workflow_name, template_name, buddy_block_name, block_name, block_inputs,
                      block_infos):
        return self.templates[template_name].preempt_block(request_id, workflow_name, template_name, buddy_block_name,
                                                           block_name, block_inputs, block_infos)
        pass
    
    def send_shm_packet_descriptor(self, request_id: str, workflow_name: str, template_name: str, 
                                   block_name: str, function_id: int, data: bytes) -> Optional[int]:
        """通过共享内存发送数据包描述符"""
        if not self.shm_enabled or not self.shm_manager:
            return None
        
        try:
            packet_id = self.shm_manager.store_workflow_data(
                request_id=request_id,
                workflow_name=workflow_name,
                template_name=template_name,
                block_name=block_name,
                function_id=function_id,
                data=data
            )
            return packet_id
        except Exception as e:
            print(f"FunctionManager发送共享内存数据失败: {e}")
            return None
    
    def receive_shm_packet_descriptor(self, packet_id: int) -> Optional[tuple]:
        """通过共享内存接收数据包描述符"""
        if not self.shm_enabled or not self.shm_manager:
            return None
        
        try:
            result = self.shm_manager.retrieve_workflow_data(packet_id)
            if result:
                descriptor, data = result
                return descriptor, data
            return None
        except Exception as e:
            print(f"FunctionManager接收共享内存数据失败: {e}")
            return None
    
    def set_workflow_manager(self, workflow_manager):
        """设置工作流管理器引用"""
        for template in self.templates.values():
            template.workflow_manager = workflow_manager
    
    def free_shm_packet(self, packet_id: int) -> bool:
        """释放共享内存包"""
        if not self.shm_enabled or not self.shm_manager:
            return False
        
        try:
            return self.shm_manager.free_packet(packet_id)
        except Exception as e:
            print(f"FunctionManager释放共享内存包失败: {e}")
            return False
    
    def should_use_shm(self, data_size: int) -> bool:
        """判断是否应该使用共享内存传输"""
        if not self.shm_enabled:
            return False
        
        # 如果数据大小超过阈值，使用共享内存
        from config import config
        return data_size >= config.SHM_THRESHOLD_SIZE
