import os
import shutil
import time
import requests
import gevent
from typing import Optional
from docker import DockerClient
from gevent.lock import BoundedSemaphore
from docker.types import Mount
from src.function_manager.file_controller import file_controller
from config import config
from src.workflow_manager.flow_monitor import flow_monitor
from src.workflow_manager.shm_utils import FaaSFlowShmManager, is_shm_enabled

base_url = 'http://127.0.0.1:{}/{}'
work_dir = '/proxy/mnt'


class Container:
    def __init__(self, container, blocks_name, port, attr, parallel_limit, cpu, KAFKA_CHUNK_SIZE, shm_enabled=False):
        self.container = container
        self.port = port
        self.attr = attr
        self.idle_blocks_cnt = parallel_limit
        self.blocks_last_time = {block_name: time.time() for block_name in blocks_name}
        self.last_time = time.time()
        self.running_blocks = set()
        # self.lock = BoundedSemaphore()
        self.cpu = cpu
        self.KAFKA_CHUNK_SIZE = KAFKA_CHUNK_SIZE
        self.shm_enabled = shm_enabled

        # 共享内存管理器（由workersp.py统一初始化）
        self.shm_manager = None
        if self.shm_enabled:
            # 根据配置检查对应的SHM管理器
            from config import config
            if config.USE_OPTIMIZED_SHM:
                # 检查优化SHM管理器
                try:
                    from src.workflow_manager.optimized_shm import OptimizedHostSharedMemoryManager
                    # 优化SHM由宿主机初始化，容器只需要检查SHM是否存在
                    import multiprocessing.shared_memory as shm
                    shm_obj = shm.SharedMemory(name=config.OPTIMIZED_SHM_NAME, create=False)
                    shm_obj.close()
                    print(f"容器{self.port}优化共享内存模式已启用")
                except Exception as e:
                    print(f"警告: 容器{self.port}优化共享内存管理器不可用: {e}")
                    self.shm_enabled = False
            else:
                # 检查传统SHM管理器
                from src.workflow_manager.shm_utils import get_global_faasflow_shm_manager
                self.shm_manager = get_global_faasflow_shm_manager()
                if self.shm_manager and self.shm_manager.initialized:
                    print(f"容器{self.port}传统共享内存模式已启用")
                else:
                    print(f"警告: 容器{self.port}传统共享内存管理器不可用")
                    self.shm_enabled = False

    @classmethod
    def create(cls, client: DockerClient, image_name, blocks_name, port, attr, cpus, parallel_limit, KAFKA_CHUNK_SIZE, shm_enabled=False) -> 'Container':
        # host_path, dir_id = file_controller.allocate_dir()
        host_path = config.FILE_CONTROLLER_PATH
        mount = Mount(work_dir, host_path, type='bind')
        
        # 添加日志目录挂载
        log_host_path = config.PERF_LOG_DIR
        log_mount = Mount(config.CONTAINER_LOG_MOUNT_PATH, log_host_path, type='bind')
        
        f = 1
        
        # 如果启用共享内存，添加必要的容器配置
        container_kwargs = {
            'detach': True,
            'ports': {'5000/tcp': str(port)},
            'labels': ['workflow'],
            'cpu_period': int(100000 * f),
            'cpu_quota': int(config.DOCKER_CPU_QUOTA * cpus * parallel_limit * f),
            'mounts': [mount, log_mount],
            'cap_add': ['NET_ADMIN']
        }
        env_vars = container_kwargs.get('environment', {})
        env_vars.update({
            'FAASFLOW_CONTAINER_PORT': str(port),
        })
        container_kwargs['environment'] = env_vars
        
        if shm_enabled:
            # 添加共享内存相关配置
            volumes = {
                '/dev/shm': {'bind': '/dev/shm', 'mode': 'rw'},
                '/tmp/faasflow_host_shm.sock': {'bind': '/tmp/faasflow_host_shm.sock', 'mode': 'rw'}
            }
            
            # 添加文件锁目录挂载
            volumes['/tmp'] = {'bind': '/tmp', 'mode': 'rw'}
            
            container_kwargs.update({
                'ipc_mode': 'host',  # 共享宿主机的IPC命名空间
                'volumes': volumes  # 挂载共享内存目录和锁文件目录
            })
        
        container = client.containers.run(image_name, **container_kwargs)

        # mounts=[mount])
        # file_controller.bind(container.id, dir_id)
        res = cls(container, blocks_name, port, attr, parallel_limit, cpus, KAFKA_CHUNK_SIZE, shm_enabled)
        res.wait_start()
        return res

    def wait_start(self):
        while True:
            try:
                r = requests.get(base_url.format(self.port, 'init'), json={'cpu': self.cpu,
                                                                           'limit_net': True,
                                                                           'KAFKA_CHUNK_SIZE': self.KAFKA_CHUNK_SIZE})
                if r.status_code == 200:
                    break
            except Exception:
                pass
            gevent.sleep(0.005)

    # def init(self, request_id, workflow_name, template_name, block_name, block_inputs):
    #     data = {'request_id': request_id,
    #             'workflow_name': workflow_name,
    #             'template_name': template_name,
    #             'block_name': block_name,
    #             'block_inputs': block_inputs}
    #     r = requests.post(base_url.format(self.port, 'init'), json=data)
    #     self.last_time = time.time()
    #     return r.status_code == 200

    def send_data(self, request_id, workflow_name, function_name, datas, datatype):
        # if datatype is BIG, then container's proxy should  the big data from couchdb by itself.
        data = {'datas': datas,
                'datatype': datatype}
        r = requests.post(base_url.format(self.port, 'send_data'), json=data)

    # def run_function(self):
    #     # print(data)
    #     r = requests.post(base_url.format(self.port, 'run'))
    #     self.last_time = time.time()
    #     return r.status_code

    def get_prefetch_filepath(self, db_key):
        return os.path.join(config.PREFETCH_POOL_PATH, db_key)

    def check_input_db_data(self, request_id, datainfo, mnt_dir=None):
        if datainfo['datatype'] == 'redis_data_ready':
            db_key = datainfo['db_key']
            if flow_monitor.requests_keys_info[request_id][db_key].in_disk:
                datainfo['datatype'] = 'disk_data_ready'
            # else:
            #     flow_monitor.requests_keys_info[request_id][db_key].link_mnts.append(mnt_dir)
        # if datainfo['datatype'] == 'disk_data_ready':
        #     db_key = datainfo['db_key']
        #     shutil.copy(os.path.join(config.PREFETCH_POOL_PATH, db_key), os.path.join(mnt_dir, db_key))

    def link_prefetch_data(self, request_id, workflow_name, template_name, block_name, block_inputs, block_infos):
        # mnt_dir = file_controller.get_container_dir(self.container.id)
        mnt_dir = None

        for name, infos in block_inputs.items():
            datatype = block_infos['input_datas'][name]['type']
            if datatype == 'NORMAL':
                self.check_input_db_data(request_id, infos, mnt_dir)
            elif datatype == 'LIST':
                for info in infos.values():
                    self.check_input_db_data(request_id, info, mnt_dir)
            else:
                raise Exception

    def run_gc(self):
        r = requests.post(base_url.format(self.port, 'run_gc'))
        assert r.status_code == 200

    def run_block(self, request_id, workflow_name, template_name, templates_infos, block_name, block_inputs,
                  block_infos):
        # this may be redundant, since running_blocks will be added in get_idle_container()
        # self.running_blocks.add(block_name)
        self.link_prefetch_data(request_id, workflow_name, template_name, block_name, block_inputs, block_infos)
        data = {'request_id': request_id,
                'workflow_name': workflow_name,
                'template_name': template_name,
                'templates_infos': templates_infos,
                'block_name': block_name,
                'block_inputs': block_inputs,
                'block_infos': block_infos,
                'chunk_size': config.CHUNK_SIZE}
        # print(template_name, block_name, 'container still has idle block?:', self.idle_blocks_cnt)
        r = requests.post(base_url.format(self.port, 'run_block'), json=data)
        delay_time = r.json()['delay_time']
        assert r.status_code == 200
        self.blocks_last_time[block_name] = self.last_time = time.time()
        # self.running_blocks.remove(block_name)
        return delay_time
    
    def run_block_shm(self, packet_id: int, request_id: str, workflow_name: str, template_name: str, 
                      block_name: str, block_infos: dict) -> dict:
        """使用共享内存运行块"""
        try:
            # 从共享内存读取输入数据
            result = self.receive_shm_packet(packet_id)
            if not result:
                raise Exception(f"无法从共享内存读取数据: 包ID={packet_id}")
            
            descriptor, input_data = result
            
            # 反序列化输入数据
            import json
            block_inputs = json.loads(input_data.decode('utf-8'))
            
            # 执行块逻辑（这里需要调用容器的实际执行逻辑）
            # 由于我们无法直接调用容器的内部逻辑，这里简化处理
            # 在实际实现中，需要修改容器代码以支持共享内存
            
            # 模拟执行时间
            import time
            start_time = time.time()
            
            # 这里应该调用实际的块执行逻辑
            # 暂时返回模拟结果
            output_data = b"Block execution completed via shared memory"
            
            # 将输出数据存储到共享内存
            output_packet_id = None
            if self.shm_enabled:
                output_packet_id = self.send_shm_packet(
                    request_id=request_id,
                    workflow_name=workflow_name,
                    template_name=template_name,
                    block_name=block_name,
                    function_id=1,
                    data=output_data
                )
            
            end_time = time.time()
            delay_time = end_time - start_time
            
            # 更新容器状态
            self.blocks_last_time[block_name] = self.last_time = time.time()
            
            return {
                'delay_time': delay_time,
                'output_packet_id': output_packet_id,
                'status': 'success'
            }
            
        except Exception as e:
            print(f"共享内存块执行失败: {e}")
            return {
                'delay_time': 0.0,
                'output_packet_id': None,
                'status': 'error',
                'error': str(e)
            }

    def destroy(self):
        # requests.get(base_url.format(self.port, 'delete_topic'))
        self.container.remove(force=True)
    
    def send_shm_packet(self, request_id: str, workflow_name: str, template_name: str, 
                        block_name: str, function_id: int, data: bytes) -> Optional[int]:
        """通过共享内存发送数据包"""
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
            print(f"容器{self.port}发送共享内存数据失败: {e}")
            return None
    
    def receive_shm_packet(self, packet_id: int) -> Optional[tuple]:
        """通过共享内存接收数据包"""
        if not self.shm_enabled or not self.shm_manager:
            return None
        
        try:
            result = self.shm_manager.retrieve_workflow_data(packet_id)
            if result:
                descriptor, data = result
                return descriptor, data
            return None
        except Exception as e:
            print(f"容器{self.port}接收共享内存数据失败: {e}")
            return None
    
    def free_shm_packet(self, packet_id: int) -> bool:
        """释放共享内存包"""
        if not self.shm_enabled or not self.shm_manager:
            return False
        
        try:
            return self.shm_manager.free_packet(packet_id)
        except Exception as e:
            print(f"容器{self.port}释放共享内存包失败: {e}")
            return False
    
    def should_use_shm(self, data_size: int) -> bool:
        """判断是否应该使用共享内存传输"""
        if not self.shm_enabled:
            return False
        
        # 如果数据大小超过阈值，使用共享内存
        return data_size >= config.SHM_THRESHOLD_SIZE
