import time

from typing import List, Dict, Optional

import gevent
from config import config
from src.function_manager.container import Container
from src.function_manager.template_info import TemplateInfo
from gevent.lock import BoundedSemaphore
from src.function_manager.port_manager import PortManager
from src.workflow_manager.repository import Repository

repo = Repository()


class RequestInfo:
    def __init__(self, request_id, workflow_name, template_name, templates_infos, block_name, block_inputs,
                 block_infos):
        self.request_id = request_id
        self.workflow_name = workflow_name
        self.template_name = template_name
        self.templates_infos = templates_infos
        self.block_name = block_name
        self.block_inputs = block_inputs
        self.block_infos = block_infos
        # self.result = event.AsyncResult()
        self.arrival_time = time.time()


idle_lifetime = 600


class Template:
    def __init__(self, client, template_info: TemplateInfo, port_manager: PortManager, parallel_limit, cpus, shm_enabled=False):
        self.client = client
        self.template_info = template_info
        self.port_manager = port_manager
        self.parallel_limit = parallel_limit
        self.cpus = cpus
        self.shm_enabled = shm_enabled

        self.KAFKA_CHUNK_SIZE = None
        kafka_config = repo.get_kafka_config()
        if kafka_config is not None:
            self.KAFKA_CHUNK_SIZE = kafka_config['KAFKA_CHUNK_SIZE']
        self.num_processing = 0
        self.request_queue: List[RequestInfo] = []
        # lock may be useless!
        self.lock = BoundedSemaphore()
        self.num_exec = 0
        self.idle_blocks: Dict[str, List[Container]] = {block_name: [] for block_name in
                                                        self.template_info.blocks.keys()}
        self.idle_containers: List[Container] = []
        # Todo: this need GC.
        self.requestID_block_container = {}
        self.requestIDs_container: Dict[str, Container] = {}

    def upd(self, request_id, block_name, container: Container):
        if request_id not in self.requestID_block_container:
            self.requestID_block_container[request_id] = {}
        self.requestID_block_container[request_id][block_name] = container

    def create_container(self, block_name):
        # self.lock.acquire()
        if self.num_exec > self.template_info.max_containers:
            # self.lock.release()
            return None
        self.num_exec += 1
        # self.lock.release()
        # st = time.time()
        try:
            container = Container.create(self.client,
                                         self.template_info.image_name,
                                         self.template_info.blocks.keys(),
                                         self.port_manager.allocate(),
                                         'exec',
                                         self.cpus,
                                         self.parallel_limit,
                                         self.KAFKA_CHUNK_SIZE,
                                         self.shm_enabled)
        except Exception as e:
            print(e)
            self.num_exec -= 1
            return None
        # ed = time.time()
        # print('container cold start', ed - st)
        # self.lock.acquire()
        container.idle_blocks_cnt -= 1
        if container.idle_blocks_cnt > 0:
            self.idle_containers.append(container)
        # self.lock.release()
        # container.running_blocks.add(block_name)
        # self.init_container(container)
        return container

    def get_idle_container(self, block_name=None):
        assert block_name is not None
        res = None
        # self.lock.acquire()
        print(f"get_idle_container: {len(self.idle_containers)}")
        if len(self.idle_containers) > 0:
            res = self.idle_containers[-1]
            print(f"get_idle_container: {res.idle_blocks_cnt}")
            res.idle_blocks_cnt -= 1
            assert res.idle_blocks_cnt >= 0
            if res.idle_blocks_cnt == 0:
                self.idle_containers.pop()
        # self.lock.release()
        return res

    def put_idle_container(self, container):
        # self.lock.acquire()
        self.idle_containers.append(container)
        # self.num_exec -= 1
        # self.lock.release()

    def run_block(self, container: Container, request: RequestInfo):
        # self.upd(request.request_id, request.block_name, container)
        st = time.time()
        
        delay_time = container.run_block(request.request_id, request.workflow_name, request.template_name,
                                             request.templates_infos, request.block_name, request.block_inputs,
                                             request.block_infos)
        
        ed = time.time()
        # print(request.request_id, request.template_name, delay_time)
        if self.template_info.gc == 'True' or self.template_info.gc == True:
            container.run_gc()
        if delay_time < 0.005 or config.DISABLE_PRESSURE_AWARE:
            self.put_container(container)
        else:
            gevent.spawn_later(delay_time, self.put_container, container)
        repo.save_latency(
            {'request_id': request.request_id, 'template_name': request.template_name, 'block_name': request.block_name,
             'phase': 'use_container', 'time': ed - st, 'st': st, 'ed': ed, 'cpu': self.cpus})
    
    def should_use_shm_for_request(self, request: RequestInfo) -> bool:
        """判断是否应该为请求使用共享内存传输"""
        if not self.shm_enabled:
            return False
        
        # 检查是否包含共享内存包描述符
        for data_name, data_info in request.block_inputs.items():
            if isinstance(data_info, dict) and 'datatype' in data_info:
                if data_info['datatype'] == 'entity' and 'val' in data_info:
                    val = data_info['val']
                    if isinstance(val, str) and 'shm_packet_' in val:
                        # 包含共享内存包描述符，使用共享内存模式
                        return True
        
        # 检查是否是第一个block（没有共享内存包描述符的输入）
        # 如果是第一个block，强制使用共享内存模式，这样它的输出才能存储到共享内存
        has_shm_input = False
        for data_name, data_info in request.block_inputs.items():
            if isinstance(data_info, dict) and 'datatype' in data_info:
                if data_info['datatype'] == 'entity' and 'val' in data_info:
                    val = data_info['val']
                    if isinstance(val, str) and 'shm_packet_' in val:
                        has_shm_input = True
                        break
        
        # 如果没有共享内存输入，说明是第一个block，强制使用共享内存模式
        if not has_shm_input:
            print(f"第一个block，强制使用共享内存模式: {request.template_name}.{request.block_name}")
            return True
        
        # 计算输入数据的总大小
        total_size = 0
        for data_name, data_info in request.block_inputs.items():
            if isinstance(data_info, dict) and 'datatype' in data_info:
                if data_info['datatype'] == 'entity':
                    # 实体数据，计算大小
                    if 'val' in data_info:
                        val = data_info['val']
                        if isinstance(val, str):
                            total_size += len(val.encode('utf-8'))
                        elif isinstance(val, bytes):
                            total_size += len(val)
                        elif isinstance(val, (dict, list)):
                            total_size += len(str(val).encode('utf-8'))
                elif data_info['datatype'] == 'disk_data_ready':
                    # 磁盘数据，假设较大
                    total_size += 1024 * 1024  # 假设1MB
        
        return self.should_use_shm(total_size)
    
    def run_block_with_shm(self, container: Container, request: RequestInfo) -> float:
        """使用共享内存运行块"""
        try:
            # 检查是否有共享内存描述符
            if hasattr(request, 'shm_descriptor'):
                # 使用已有的共享内存描述符
                descriptor_data = request.shm_descriptor
                packet_id = descriptor_data['packet_id']
            else:
                # 序列化输入数据
                import json
                input_data = json.dumps(request.block_inputs).encode('utf-8')
                
                # 通过共享内存发送数据
                packet_id = self.send_data_to_container_shm(
                    container, 
                    request.request_id, 
                    request.workflow_name, 
                    request.template_name, 
                    request.block_name, 
                    1,  # function_id，这里简化处理
                    input_data
                )
                
                if packet_id is None:
                    print(f"共享内存发送失败，回退到HTTP模式: {request.request_id}")
                    return container.run_block(request.request_id, request.workflow_name, request.template_name,
                                               request.templates_infos, request.block_name, request.block_inputs,
                                               request.block_infos)
                
                # 发送包描述符给容器
                descriptor_data = {
                    'packet_id': packet_id,
                    'request_id': request.request_id,
                    'workflow_name': request.workflow_name,
                    'template_name': request.template_name,
                    'block_name': request.block_name,
                    'block_infos': request.block_infos,
                    'use_shm': True
                }
            
            # 通过HTTP发送描述符（轻量级）
            import requests
            response = requests.post(
                f'http://127.0.0.1:{container.port}/run_block_shm',
                json=descriptor_data,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"容器执行失败，回退到HTTP模式: {request.request_id}")
                # 清理共享内存
                if not hasattr(request, 'shm_descriptor'):
                    self.free_shm_packet(packet_id)
                return container.run_block(request.request_id, request.workflow_name, request.template_name,
                                           request.templates_infos, request.block_name, request.block_inputs,
                                           request.block_infos)
            
            result = response.json()
            delay_time = result.get('delay_time', 0.0)
            
            # 处理输出数据（如果有的话）
            output_packet_id = result.get('output_packet_id')
            if output_packet_id:
                output_result = self.receive_data_from_container_shm(container, output_packet_id)
                if output_result:
                    descriptor, output_data = output_result
                    # 这里可以处理输出数据
                    print(f"收到容器输出数据: {len(output_data)} 字节")
                    # 清理输出包
                    self.free_shm_packet(output_packet_id)
            
            # 清理输入包
            if not hasattr(request, 'shm_descriptor'):
                self.free_shm_packet(packet_id)
            
            return delay_time
            
        except Exception as e:
            print(f"共享内存执行失败，回退到HTTP模式: {e}")
            return container.run_block(request.request_id, request.workflow_name, request.template_name,
                                       request.templates_infos, request.block_name, request.block_inputs,
                                       request.block_infos)

    def send_data(self, request_id, workflow_name, function_name, datas, datatype):
        self.requestIDs_container[request_id].send_data(request_id, workflow_name, function_name, datas, datatype)

    def allocate_block(self, request_id, workflow_name, template_name, templates_infos, block_name, block_inputs,
                       block_infos):
        request = RequestInfo(request_id, workflow_name, template_name, templates_infos, block_name, block_inputs,
                              block_infos)
        self.request_queue.append(request)
    
    def allocate_block_shm(self, request_id, workflow_name, template_name, templates_infos, block_name, descriptor_data,
                           block_infos):
        """使用共享内存分配块"""
        # 创建一个特殊的RequestInfo，包含共享内存描述符
        request = RequestInfo(request_id, workflow_name, template_name, templates_infos, block_name, {}, block_infos)
        request.shm_descriptor = descriptor_data  # 添加共享内存描述符
        self.request_queue.append(request)

    def preempt_block(self, request_id, workflow_name, template_name, buddy_block_name, block_name, block_inputs,
                      block_infos):
        container: Container = self.requestID_block_container[request_id][buddy_block_name]
        self.lock.acquire()
        if block_name not in container.running_blocks:
            container.running_blocks.add(block_name)
            self.lock.release()
            print('preempt_block_success!->', request_id, workflow_name, template_name, block_name, '--[buddy_block]<-',
                  buddy_block_name)
            gevent.spawn(container.run_block, request_id, workflow_name, template_name, block_name, block_inputs,
                         block_infos)
            return True
        else:
            self.lock.release()
            return False
        pass

    # def run_function(self, request_id, workflow_name, function_name):
    #     self.requestIDs_container[request_id].run_function()
    #     print(function_name, workflow_name, request_id, ' finished!')
    #     self.put_idle_container(self.requestIDs_container[request_id])

    # def send_request(self, request_id, workflow_name, input_data, output_data):
    #     print('send_request() in function.py', request_id, self.function_info.function_name,
    #           time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    #     data = {'request_id': request_id,
    #             'workflow_name': workflow_name,
    #             'input_data': input_data,
    #             'output_data': output_data}
    #     request = RequestInfo(request_id, data)
    #     self.request_queue.append(request)
    #     # res = request.result.get()
    #     # return res

    # def allocate(self, request_id, workflow_name, function_name, function_info):
    #     data = {'request_id': request_id,
    #             'workflow_name': workflow_name,
    #             'function_name': function_name,
    #             'function_info': function_info}
    #     request = RequestInfo(request_id, workflow_name, function_name, function_info)
    #     self.request_queue.append(request)

    def put_container(self, container: Container):
        container.idle_blocks_cnt += 1
        assert container.idle_blocks_cnt > 0
        if container.idle_blocks_cnt == 1:
            self.idle_containers.append(container)

    def dispatch_request(self):
        # if self.num_processing >= len(self.request_queue):
        #     return
        if len(self.request_queue) == 0:
            return
        request = self.request_queue.pop(0)
        # print('Allocating a block...')

        container = self.get_idle_container(request.block_name)

        if container is None:
            container = self.create_container(request.block_name)
        else:
            pass
            # print(request.block_name, request.template_name, request.request_id, 'is using idle block')

        if container is None:
            print('dispatch_failed in template.py')
            self.request_queue.append(request)
            return
        # self.num_processing -= 1

        self.run_block(container, request)
        # self.lock.acquire()
        # container.idle_blocks_cnt += 1
        # assert container.idle_blocks_cnt > 0
        # if container.idle_blocks_cnt == 1:
        #     self.idle_containers.append(container)
        # self.lock.release()
        # self.requestIDs_container[request.request_id] = container
        # request_status_code = container.send_request(request)
        #
        # self.put_idle_container(container)

    def regular_clean(self):
        outdated_containers = []
        left_idle_containers = []
        self.lock.acquire()
        now_time = time.time()
        pos = len(self.idle_containers)
        for i, container in enumerate(self.idle_containers):
            if now_time - container.last_time > idle_lifetime:
                assert container.idle_blocks_cnt == self.parallel_limit
                # if len(container.running_blocks) == 0:
                #     outdated_containers.append(container)
                # else:
                #     left_idle_containers.append(container)
            else:
                pos = i
                break
        self.num_exec -= pos
        outdated_containers.extend(self.idle_containers[:pos])
        self.idle_containers = self.idle_containers[pos:]
        self.lock.release()

        for container in outdated_containers:
            self.remove_container(container)

    def remove_container(self, container: Container):
        container.destroy()
        self.port_manager.put(container.port)
    
    def send_data_to_container_shm(self, container: Container, request_id: str, workflow_name: str, 
                                   template_name: str, block_name: str, function_id: int, data: bytes) -> Optional[int]:
        """通过共享内存向容器发送数据"""
        if not self.shm_enabled:
            return None
        
        try:
            return container.send_shm_packet(request_id, workflow_name, template_name, block_name, function_id, data)
        except Exception as e:
            print(f"Template发送共享内存数据失败: {e}")
            return None
    
    def receive_data_from_container_shm(self, container: Container, packet_id: int) -> Optional[tuple]:
        """通过共享内存从容器接收数据"""
        if not self.shm_enabled:
            return None
        
        try:
            return container.receive_shm_packet(packet_id)
        except Exception as e:
            print(f"Template接收共享内存数据失败: {e}")
            return None
    
    def should_use_shm(self, data_size: int) -> bool:
        """判断是否应该使用共享内存传输"""
        if not self.shm_enabled:
            return False
        
        # 如果数据大小超过阈值，使用共享内存
        from config import config
        return data_size >= config.SHM_THRESHOLD_SIZE
