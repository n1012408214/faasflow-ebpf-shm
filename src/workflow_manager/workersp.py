import copy
import json
import os.path
import time
import gevent
import gevent.lock
import gevent.event
import logging
from typing import Dict, List, Optional
import couchdb
import requests

logger = logging.getLogger(__name__)
from src.workflow_manager.flow_monitor import flow_monitor
from workflow_info import WorkflowInfo
from request_info import RequestInfo
from src.function_manager.function_manager import FunctionManager
from config import config
import pykafka
from src.workflow_manager.repository import Repository
from src.workflow_manager.shm_utils import FaaSFlowShmManager, is_shm_enabled, get_shm_config, get_global_faasflow_shm_manager
from src.workflow_manager.simple_perf_logger import start_request_timer, end_request_timer, log_event, log_block_execution, log_data_transfer, log_container_start, log_container_ready

repo = Repository()

# Todo: what is the best interval for dispatch?
dispatch_interval = 0.0005
min_port = 20000

prefetch_dir = config.PREFETCH_POOL_PATH


class WorkflowState:
    def __init__(self, request_id, raw_data):
        self.request_id = request_id
        self.templates_blocks_inputDatas: Dict[str, Dict[str, dict]] = {}
        self.templates_blocks_input_cnt = {}
        self.templates_blocks_triggered = set()
        self.lock = gevent.lock.BoundedSemaphore()
        self.prefetch_keys_status: Dict[str, gevent.event.AsyncResult] = {}
        # self.datas_type = {}
        self.containers_state = {}

        for template_name, template_infos in raw_data['templates'].items():
            self.containers_state[template_name] = 'NA'
            self.templates_blocks_input_cnt[template_name] = {}
            self.templates_blocks_inputDatas[template_name] = {}
            for block_name, block_infos in template_infos['blocks'].items():
                self.templates_blocks_input_cnt[template_name][block_name] = 0
                self.templates_blocks_inputDatas[template_name][block_name] = {}
                for input_data_name, input_data_infos in block_infos['input_datas'].items():
                    input_data_type = input_data_infos['type']
                    # if input_data_type == 'NORMAL':
                    #     self.templates_blocks_inputDatas[template_name][block_name][input_data_name] = None
                    # elif input_data_type == 'LIST':
                    #     self.templates_blocks_inputDatas[template_name][block_name][input_data_name] = []


class DataInfo:
    def __init__(self, request_id, workflow_name, template_name, block_name, data_name, data_infos, from_local,
                 from_virtual=None):
        self.request_id = request_id
        self.workflow_name = workflow_name
        self.template_name = template_name
        self.block_name = block_name
        self.data_name = data_name
        self.data_infos = data_infos
        # This data need to be forwarded to other node iff from_local is True.
        self.from_local = from_local
        self.from_virtual = from_virtual


class WorkerSPManager:
    def __init__(self, host_addr, gateway_addr, workflows_info_path: dict, functions_info_path: str):
        global min_port
        self.host_addr = host_addr
        self.gateway_addr = gateway_addr
        self.db = couchdb.Server(config.COUCHDB_URL)['results']
        self.requests_info: Dict[str, RequestInfo] = {}

        self.workflows_state: Dict[str, WorkflowState] = {}
        self.workflows_info = WorkflowInfo.parse(workflows_info_path)
        self.function_manager = FunctionManager(min_port, functions_info_path)
        # 将工作流管理器传递给FunctionManager
        self.function_manager.set_workflow_manager(self)
        self.flow_monitor = flow_monitor
        self.incoming_data_queue: List[DataInfo] = []
        self.lock = gevent.lock.BoundedSemaphore()
        self.kafka_client = pykafka.KafkaClient(hosts=config.KAFKA_URL, use_greenlets=True)
        
        # 初始化共享内存管理器（统一初始化点）
        self.shm_manager = None
        self.shm_enabled = is_shm_enabled()
        print(f"SHM启用检查: {self.shm_enabled}")
        if self.shm_enabled:
            # 获取全局SHM管理器实例并初始化
            self.shm_manager = get_global_faasflow_shm_manager()
            if not self.shm_manager.initialized:
                # 设置配置参数（根据配置选择SHM版本）
                if getattr(config, 'USE_OPTIMIZED_SHM', False):
                    self.shm_manager.shm_name = config.OPTIMIZED_SHM_NAME
                    self.shm_manager.shm_size = config.OPTIMIZED_SHM_SIZE
                else:
                    self.shm_manager.shm_name = config.SHM_NAME
                    self.shm_manager.shm_size = config.SHM_SIZE
                # 初始化SHM池（Socket服务器已在init_shm_pool中启动）
                if not self.shm_manager.init_shm_pool():
                    print("警告: 共享内存初始化失败，将使用HTTP模式")
                    self.shm_enabled = False
                else:
                    print(f"共享内存模式已启用: {get_shm_config()}")
            else:
                print(f"共享内存模式已启用（复用现有实例）: {get_shm_config()}")
        
        gevent.spawn_later(dispatch_interval, self.dispatch_incoming_data)
        # min_port += 5000

    def init_incoming_request(self, request_id, workflow_name, templates_info: Dict[str, dict]):
        ips = set()
        for template_info in templates_info.values():
            ips.add(template_info['ip'])
        self.requests_info[request_id] = RequestInfo(workflow_name, ips, templates_info)
        self.workflows_state[request_id] = WorkflowState(request_id, self.workflows_info[workflow_name].data)
        self.flow_monitor.requests_keys_info[request_id] = {}

    def receive_incoming_data(self, request_id, workflow_name, template_name, block_name, datas: dict,
                              from_local: bool, from_virtual=None):
        # datatype = 'entity' or 'metadata'
        # print("Receive data: {}, from request: {} workflow: {}".format(datas, request_id, workflow_name))
        if request_id not in self.workflows_state:
            raise Exception('Error: workflow_state is not inited yet, it should be init when received request_info.')
        # for k in datas.keys():
        #     if k in self.workflows_state[request_id].datas:
        #         raise Exception('Panic: received an already received data!')
        # datas_successors = self.workflows_info[workflow_name].datas_successors
        # functions_infos = self.requests_info[request_id].functions_infos
        assert from_virtual != 'None'
        for data_name, data_infos in datas.items():
            self.incoming_data_queue.append(
                DataInfo(request_id, workflow_name, template_name, block_name, data_name, data_infos, from_local,
                         from_virtual))
        # self.workflows_state[request_id].datas[k] = v
        # self.workflows_state[request_id].datas_type[k] = datatype
        # if '$USER' in k and from_local and datatype == 'entity': # assert user_data which is sent to user is small
        #     gevent.spawn(self.post_user_data, request_id, k, v)
        # local_functions = set()
        # already_transfered_ips = set()
        # for function_name in datas_successors[k]:
        #     target_ip = functions_infos[function_name]['ip']
        #     if target_ip == self.host_addr or target_ip == '127.0.0.1':
        #         local_functions.add(function_name)
        #     elif from_local and target_ip not in already_transfered_ips:
        #         already_transfered_ips.add(target_ip)
        #         gevent.spawn(self.send_data_remote, target_ip, request_id, workflow_name, {k: v}, datatype)
        # if len(local_functions) > 0:
        #     self.incoming_data_queue.append(DataInfo(request_id, workflow_name, k, v, datatype, local_functions))

    def post_user_data(self, request_id, k, v):
        remote_url = 'http://{}/post_user_data'.format(self.gateway_addr)
        data = {'request_id': request_id,
                'datas': {k: v}}
        requests.post(remote_url, json=data)

    def get_datatype(self, workflow_name, template_name, block_name, input_or_output, data_name):
        if input_or_output == 'input':
            return self.workflows_info[workflow_name].templates_infos[template_name]['blocks'][block_name][
                'input_datas'][data_name]['type']
        elif input_or_output == 'output':
            return self.workflows_info[workflow_name].templates_infos[template_name]['blocks'][block_name][
                'output_datas'][data_name]['type']
        else:
            raise Exception('in_or_out: ', input_or_output)

    def flow_data(self, request_id, workflow_name, dest_template_name, dest_block_name, dest_data_name, data_infos,
                  offset=None):
        # Todo: metadata message may overwrite bigdata_ready message
        # print('flow_data->', request_id, workflow_name, dest_template_name, dest_block_name, dest_data_name, data_infos)
        output_type = data_infos['output_type']
        workflow_state = self.workflows_state[request_id]
        if output_type == 'FOREACH':
            # workflow_state.lock.acquire()
            # if dest_data_name not in workflow_state.templates_blocks_inputDatas[dest_template_name][dest_block_name]:
            #     workflow_state.templates_blocks_inputDatas[dest_template_name][dest_block_name][dest_data_name] = []
            # workflow_state.lock.release()
            # workflow_state.templates_blocks_inputDatas[dest_template_name][dest_block_name][dest_data_name].append(
            #     data_infos)

            # Todo. Currently assume foreach Vars can directly trigger, i.e. No other on-trip Vars.
            input_datas = {dest_data_name: data_infos}
            for k, v in workflow_state.templates_blocks_inputDatas[dest_template_name][dest_block_name].items():
                if k != dest_data_name:
                    input_datas[k] = v
            self.trigger_normal(request_id, workflow_name, dest_template_name, dest_block_name, input_datas)
            return
        elif output_type == 'MERGE':
            input_datas = workflow_state.templates_blocks_inputDatas[dest_template_name][dest_block_name]
            # workflow_state.lock.acquire()
            if dest_data_name not in input_datas:
                input_datas[dest_data_name] = {}
            data_infos['output_type'] = 'NORMAL'
            while 'VIRTUAL.CNT' not in input_datas:
                gevent.sleep(0.003)
            input_datas[dest_data_name][data_infos['serial_num']] = data_infos
            #print('VIRTUAL.CNT:'+str(input_datas['VIRTUAL.CNT']['val']))
            if len(input_datas[dest_data_name]) == input_datas['VIRTUAL.CNT']['val']:
                # all split data has arrived, then trigger!
                workflow_state.templates_blocks_input_cnt[dest_template_name][dest_block_name] += 1
                if workflow_state.templates_blocks_input_cnt[dest_template_name][dest_block_name] == len(
                        self.workflows_info[workflow_name].templates_infos[dest_template_name]['blocks'][
                            dest_block_name][
                            'input_datas']):
                    # workflow_state.lock.release()
                    self.trigger_normal(request_id, workflow_name, dest_template_name, dest_block_name)
                else:
                    pass
                    # workflow_state.lock.release()
            else:
                pass
                # workflow_state.lock.release()
            return
        dest_data_type = self.get_datatype(workflow_name, dest_template_name, dest_block_name, 'input', dest_data_name)
        assert dest_data_type == 'NORMAL'
        if dest_data_type == 'NORMAL':
            # repeated input does not count!
            if dest_data_name in workflow_state.templates_blocks_inputDatas[dest_template_name][dest_block_name]:
                return
            workflow_state.templates_blocks_inputDatas[dest_template_name][dest_block_name][dest_data_name] = data_infos
        elif dest_data_type == 'LIST':
            target: list = workflow_state.templates_blocks_inputDatas[dest_template_name][dest_block_name][
                dest_data_name]
            assert offset is not None
            while len(target) < offset + 1:
                target.append(None)
            target[offset] = data_infos
        workflow_state.lock.acquire()
        workflow_state.templates_blocks_input_cnt[dest_template_name][dest_block_name] += 1
        if workflow_state.templates_blocks_input_cnt[dest_template_name][dest_block_name] == len(
                self.workflows_info[workflow_name].templates_infos[dest_template_name]['blocks'][dest_block_name][
                    'input_datas']):
            workflow_state.lock.release()
            self.trigger_block_local(request_id, workflow_name, dest_template_name, dest_block_name)
        else:
            workflow_state.lock.release()

    def prefetch_kafka_data(self, datainfo: DataInfo):
        # st = time.time()
        requests.post(config.PREFETCHER_URL.format('prefetch_data'), json=datainfo.data_infos)
        db_key = datainfo.data_infos['db_key']
        self.workflows_state[datainfo.request_id].prefetch_keys_status[db_key].set(1)
        # ed = time.time()
        # print('trying saving latency for kafka fetch', datainfo.request_id)
        # repo.save_latency({'request_id': datainfo.request_id, 'template_name': datainfo.template_name,
        #                    'block_name': datainfo.block_name + '_prefetch_Real_time_',
        #                    'phase': 'use_container', 'time': ed - st, 'st': st, 'ed': ed})
        return
        db_key = datainfo.data_infos['db_key']
        partition_idx = datainfo.data_infos['partition_idx']
        chunk_num = datainfo.data_infos['chunk_num']
        topic = datainfo.data_infos['topic']
        start_offset = datainfo.data_infos['start_offset']
        request_id = datainfo.request_id
        # doc = self.db[request_id]
        while True:
            try:
                kafka_topic: pykafka.topic.Topic = self.kafka_client.topics[topic]
                break
            except Exception as e:
                print(e)
                gevent.sleep(0.1)
        kafka_consumer = kafka_topic.get_simple_consumer(partitions=[kafka_topic.partitions[partition_idx]])
        # print('----', kafka_topic.partitions, kafka_topic.earliest_available_offsets(),
        #       kafka_topic.latest_available_offsets(), kafka_consumer.held_offsets)
        while True:
            try:
                tmp = kafka_topic.latest_available_offsets()[partition_idx].offset[0]
                break
            except Exception as e:
                print(e)
                gevent.sleep(0.1)

        while tmp < start_offset:
            print(tmp, start_offset)
            gevent.sleep(0.1)
            tmp = kafka_topic.latest_available_offsets()[partition_idx].offset[0]
        if start_offset > 0:
            kafka_consumer.reset_offsets([(kafka_topic.partitions[partition_idx], start_offset - 1)])
        st = time.time()
        size = 0
        with open(os.path.join(prefetch_dir, db_key), 'wb') as f:
            for i in range(chunk_num):
                chunk_data = kafka_consumer.consume().value
                # if i == 0:
                #     st = time.time()
                # tmp = json.loads(chunk_data)
                # print(db_key, tmp['db_key'], tmp['idx'], i, chunk_num)
                # chunk_data = tmp['val'].encode()
                size += len(chunk_data)
                f.write(chunk_data)
        ed = time.time()

        print('prefetch time:', ed - st, 'file size:', size)
        self.workflows_state[datainfo.request_id].prefetch_keys_status[db_key].set(1)
        repo.save_latency(
            {'request_id': datainfo.request_id, 'template_name': datainfo.template_name,
             'block_name': datainfo.block_name + '_prefetch_Real_time',
             'phase': 'use_container', 'time': ed - st, 'st': st, 'ed': ed})

    def prefetch_couch_data(self, datainfo: DataInfo):
        request_id = datainfo.request_id
        db_key = datainfo.data_infos['db_key']
        attachment = self.db.get_attachment(request_id, filename=db_key)
        st = time.time()
        with open(os.path.join(prefetch_dir, db_key), 'wb') as f:
            data = attachment.read()
            size = len(data)
            f.write(data)
        ed = time.time()
        print('prefetch time:', ed - st, 'file size:', size)
        self.workflows_state[datainfo.request_id].prefetch_keys_status[db_key].set(1)

    def prefetch_data(self, datainfo: DataInfo):
        st = time.time()
        datatype = datainfo.data_infos['datatype']
        if datatype == 'kafka_data_ready':
            self.prefetch_kafka_data(datainfo)
        elif datatype == 'couch_data_ready':
            self.prefetch_couch_data(datainfo)
        else:
            raise Exception
        ed = time.time()
        # repo.save_latency(
        #     {'request_id': datainfo.request_id, 'template_name': datainfo.template_name,
        #      'block_name': datainfo.block_name + '_prefetch_Total_time',
        #      'phase': 'use_container', 'time': ed - st, 'st': st, 'ed': ed})

    def get_dest(self, data: DataInfo) -> dict:
        block_info = self.workflows_info[data.workflow_name].templates_infos[data.template_name]['blocks'][
            data.block_name]
        if block_info['type'] == 'SWITCH':
            return block_info['conditions'][data.data_infos['switch_branch']][data.data_name]
        elif block_info['type'] == 'NORMAL':
            return block_info['output_datas'][data.data_name]['dest']
        else:
            raise Exception

    def handle_data_fetched(self, data: DataInfo):
        self.flow_monitor.decrease_key_dependencies(data.request_id, data.data_infos['db_key'])

    def handle_redis_data_ready(self, data: DataInfo):
        request_id = data.request_id
        request_info = self.requests_info[request_id]
        data_infos = data.data_infos
        db_key = data_infos['db_key']

        self.flow_monitor.add_key(request_id, db_key, config.REDIS_EXPIRE_SECONDS, data_infos['local_cnt'], in_redis=True, in_disk=False,
                                  in_couchDB=data_infos['remote_cnt'] > 0, datasize=data_infos['datasize'])

        dest = self.get_dest(data)
        for dest_template_name, dest_template_infos in dest.items():
            if dest_template_name == '$USER':
                gevent.spawn(self.post_user_data, request_id, data.data_name, data_infos)
                continue
            target_ip = request_info.templates_infos[dest_template_name]['ip']
            # Only trigger local
            if target_ip == self.host_addr or target_ip == '127.0.0.1':
                for dest_block_name, dest_block_infos in dest_template_infos.items():
                    for dest_data_name in dest_block_infos:
                        self.flow_data(request_id, data.workflow_name, dest_template_name, dest_block_name,
                                       dest_data_name, data_infos)

    def handle_remote_data_ready(self, data: DataInfo):
        request_id = data.request_id
        request_info = self.requests_info[request_id]
        data_infos = data.data_infos
        dest = self.get_dest(data)
        if data.from_local:
            transfered_ips = set()
            for dest_template_name in dest:
                if dest_template_name == '$USER':
                    gevent.spawn(self.post_user_data, request_id, data.data_name, data_infos)
                    continue
                target_ip = request_info.templates_infos[dest_template_name]['ip']
                # Send to remote
                if target_ip != self.host_addr and target_ip != '127.0.0.1' and target_ip not in transfered_ips:
                    transfered_ips.add(target_ip)
                    gevent.spawn(self.send_data_remote, target_ip, request_id, data.workflow_name, data.template_name,
                                 data.block_name, {data.data_name: data_infos}, data.from_virtual)
        else:
            # After receive couchDB data from remote, trigger local
            workflow_state = self.workflows_state[request_id]
            db_key = data_infos['db_key']
            self.flow_monitor.add_key(request_id, db_key, -1, local_cnt=data_infos['ips_cnt'][self.host_addr],
                                      in_redis=False, in_disk=True, in_couchDB=True)
            if db_key not in workflow_state.prefetch_keys_status:
                workflow_state.prefetch_keys_status[db_key] = gevent.event.AsyncResult()
                gevent.spawn(self.prefetch_data, data)
            for dest_template_name, dest_template_infos in dest.items():
                target_ip = request_info.templates_infos[dest_template_name]['ip']
                if target_ip == self.host_addr or target_ip == '127.0.0.1':
                    for dest_block_name, dest_block_infos in dest_template_infos.items():
                        for dest_data_name in dest_block_infos:
                            self.flow_data(request_id, data.workflow_name, dest_template_name, dest_block_name,
                                           dest_data_name, data_infos)

    def dispatch_incoming_data(self):
        gevent.spawn_later(dispatch_interval, self.dispatch_incoming_data)
        if len(self.incoming_data_queue) > 0:
            data = self.incoming_data_queue.pop(0)
        else:
            return
        request_id = data.request_id
        data_infos = data.data_infos
        datatype = data_infos['datatype']
        if datatype == 'data_fetched':
            # print('-----handle_data_fetched----------')
            self.handle_data_fetched(data)
            return
        if datatype == 'redis_data_ready':
            self.handle_redis_data_ready(data)
            return
        if datatype == 'couch_data_ready' or datatype == 'kafka_data_ready':
            self.handle_remote_data_ready(data)
            return
        request_info = self.requests_info[request_id]

        workflow_name = data.workflow_name
        workflow_info = self.workflows_info[workflow_name]

        data_name = data.data_name
        data_infos['from_template_name'] = data.template_name
        data_infos['from_block_name'] = data.block_name
        if data.template_name == '$USER' or data.template_name == 'global_inputs':
            st = time.time()
            global_inputs = workflow_info.data['global_inputs']
            for dest_template_name, dest_template_infos in global_inputs[data_name]['dest'].items():
                target_ip = request_info.templates_infos[dest_template_name]['ip']
                if target_ip == self.host_addr or target_ip == '127.0.0.1':
                    for dest_block_name, dest_block_infos in dest_template_infos.items():
                        for dest_data_name, offset in dest_block_infos.items():
                            self.flow_data(request_id, workflow_name, dest_template_name, dest_block_name,
                                           dest_data_name, data_infos, offset)
            ed = time.time()
            # repo.save_latency({'request_id': request_id, 'template_name': data.template_name,
            #                    'block_name': data.block_name + '_flow_data_time',
            #                    'phase': 'use_container', 'time': ed - st, 'st': st, 'ed': ed})
        else:
            already_transfered_ips = set()
            dest = None
            block_info = workflow_info.templates_infos[data.template_name]['blocks'][data.block_name]
            if block_info['type'] == 'SWITCH':
                dest = block_info['conditions'][data_infos['switch_branch']][data_name]
            elif block_info['type'] == 'NORMAL':
                dest = block_info['output_datas'][data_name]['dest']
            else:
                raise Exception('undefined block type: ', block_info['type'])
            prefetched_status = 'NA'
            for dest_template_name, dest_template_infos in dest.items():
                if dest_template_name == '$USER':
                    if data_infos['datatype'] != 'metadata':
                        # Todo: regular clean should be triggered by the gateway
                        # gevent.spawn_later(5, self.clean_request, request_id)
                        gevent.spawn(self.post_user_data, request_id, data_name, data_infos)
                    continue
                target_ip = request_info.templates_infos[dest_template_name]['ip']
                if target_ip == self.host_addr or target_ip == '127.0.0.1':
                    # if data_infos['datatype'] == 'metadata':
                    #     # continue
                    #     # Try affinity scheduling
                    #     if dest_template_name == data.template_name:
                    #         for dest_block_name, dest_block_infos in dest_template_infos.items():
                    #             for dest_data_name, offset in dest_block_infos.items():
                    #                 self.flow_data(request_id, workflow_name, dest_template_name, dest_block_name,
                    #                                dest_data_name, data_infos, offset)
                    #     continue
                    for dest_block_name, dest_block_infos in dest_template_infos.items():
                        for dest_data_name, offset in dest_block_infos.items():
                            self.flow_data(request_id, workflow_name, dest_template_name, dest_block_name,
                                           dest_data_name, data_infos, offset)
                elif data.from_local is True and data_infos['datatype'] != 'metadata':
                    if target_ip not in already_transfered_ips:
                        already_transfered_ips.add(target_ip)
                        gevent.spawn(self.send_data_remote, target_ip, request_id, workflow_name, data.template_name,
                                     data.block_name, {data_name: data_infos}, data.from_virtual)

    def clean_request(self, request_id):
        self.workflows_state.pop(request_id)
        self.flow_monitor.requests_keys_info.pop(request_id)

    def get_state(self, request_id: str):
        if request_id not in self.workflows_state:
            raise Exception('Error: trying to get a nonexistent state!')
        return self.workflows_state[request_id]

    def trigger_block_local(self, request_id, workflow_name, template_name, block_name):
        # Always trigger locally
        # print('trigger_block_local', request_id, workflow_name, template_name, block_name)

        # check whether we need to use affinity.
        input_datas = self.workflows_state[request_id].templates_blocks_inputDatas[template_name][block_name]
        if template_name + '.' + block_name in self.workflows_state[request_id].templates_blocks_triggered:
            return

        # Todo: Disable affinity trigger for now.
        # for name, infos in input_datas.items():
        #     # Todo: we only handle one metadata, multiple metadata will cause error!
        #     if 'datatype' in infos and infos['datatype'] == 'metadata':
        #         if self.trigger_affinity(request_id, workflow_name, template_name, infos['from_block_name'], block_name,
        #                                  input_datas):
        #             self.workflows_state[request_id].templates_blocks_triggered.add(template_name + '.' + block_name)
        #         return

        self.trigger_normal(request_id, workflow_name, template_name, block_name)

    # def trigger_affinity(self, request_id, workflow_name, template_name, buddy_block_name, block_name, input_datas):
    #     # Todo: In redis-cache mode, is this necessary?
    #     return self.function_manager.preempt_block(request_id, workflow_name, template_name, buddy_block_name,
    #                                                block_name, input_datas,
    #                                                self.workflows_info[workflow_name].templates_infos[template_name][
    #                                                    'blocks'][block_name])
    #     pass

    def trigger_switch(self, request_id, workflow_name, template_name, block_name):
        input_datas = self.workflows_state[request_id].templates_blocks_inputDatas[template_name][block_name]
        ctx = {}
        for data_name, data_infos in input_datas.items():
            if data_infos['datatype'] == 'entity':
                ctx[data_name] = data_infos['val']
        success = False
        for condition, output_datas in self.workflows_info[workflow_name].templates_infos[template_name]['blocks'][
            block_name]['conditions'].items():
            if eval(condition, ctx):
                success = True
                self.receive_incoming_data(request_id, workflow_name, template_name, block_name,
                                           {k: input_datas[k] for k in output_datas}, from_local=True,
                                           from_virtual={'type': 'SWITCH', 'branch': condition})
                return
        raise Exception('virtual switch branch not match')

    def check_input_db_data(self, request_id, datainfo):
        workflow_state = self.workflows_state[request_id]
        if datainfo['datatype'] in ['couch_data_ready', 'kafka_data_ready']:
            db_key = datainfo['db_key']
            workflow_state.prefetch_keys_status[db_key].get()
            datainfo['datatype'] = 'disk_data_ready'

    def check_input_datas(self, request_id, workflow_name, template_name, block_name, input_datas):
        input_datas_infos = self.workflows_info[workflow_name].templates_infos[template_name]['blocks'][block_name][
            'input_datas']
        workflow_state = self.workflows_state[request_id]
        for data_name, data_infos in input_datas.items():
            datatype = input_datas_infos[data_name]['type']
            if datatype == 'NORMAL':
                self.check_input_db_data(request_id, data_infos)
            elif datatype == 'LIST':
                for datainfo in data_infos.values():
                    self.check_input_db_data(request_id, datainfo)
            else:
                raise Exception

    def trigger_normal(self, request_id, workflow_name, template_name, block_name, input_datas=None, force_http=False):
        if input_datas is None:
            input_datas = self.workflows_state[request_id].templates_blocks_inputDatas[template_name][block_name]
        self.check_input_datas(request_id, workflow_name, template_name, block_name, input_datas)
        
        #print(f"触发block: {template_name}.{block_name}")
        #print(f"输入数据: {input_datas}")
        
        # 记录block执行开始
        start_time = time.time()
        log_event(request_id, 'BLOCK_EXECUTION_START', template_name, block_name, None, None)
        
        # 检查是否应该使用共享内存传输
        should_use_shm = self.shm_enabled 
        #print(f"是否使用共享内存: {should_use_shm}")
        
        if should_use_shm:
            #print(f"使用共享内存模式触发: {template_name}.{block_name}")
            # SHM模式和HTTP模式使用相同的触发方式，容器端会根据配置选择存储方式
            self.function_manager.allocate_block(request_id, workflow_name, template_name,
                                                 self.requests_info[request_id].templates_infos, block_name, input_datas,
                                                 self.workflows_info[workflow_name].templates_infos[template_name][
                                                     'blocks'][block_name])
        else:
            print(f"使用HTTP模式触发: {template_name}.{block_name}")
            # Todo: the ip address of template_info may be modified.
            self.function_manager.allocate_block(request_id, workflow_name, template_name,
                                                 self.requests_info[request_id].templates_infos, block_name, input_datas,
                                                 self.workflows_info[workflow_name].templates_infos[template_name][
                                                     'blocks'][block_name])
        
        # 记录block执行结束和耗时
        end_time = time.time()
        execution_time = end_time - start_time
        log_block_execution(request_id, template_name, block_name, execution_time)
        
        # 定期清理共享内存（每10次触发清理一次）
        if hasattr(self, '_trigger_count'):
            self._trigger_count += 1
        else:
            self._trigger_count = 1
        
        if self._trigger_count % 10 == 0 and self.shm_enabled and self.shm_manager:
            #print(f"定期清理共享内存")
            # 只清理已知存在的包，避免错误
            # 这里暂时禁用自动清理，避免清理错误
            pass
    
    def should_use_shm_for_block(self, input_datas: dict) -> bool:
        """判断是否应该为块使用共享内存传输"""
        if not self.shm_enabled:
            return False
        
        # 检查是否包含共享内存包描述符
        for data_name, data_info in input_datas.items():
            if isinstance(data_info, dict) and 'datatype' in data_info:
                if data_info['datatype'] == 'entity' and 'val' in data_info:
                    val = data_info['val']
                    if isinstance(val, str) and 'shm_packet_' in val:
                        # 包含共享内存包描述符，使用共享内存模式
                        return True
        
        # 检查是否是第一个block（没有共享内存包描述符的输入）
        # 如果是第一个block，强制使用共享内存模式，这样它的输出才能存储到共享内存
        has_shm_input = False
        for data_name, data_info in input_datas.items():
            if isinstance(data_info, dict) and 'datatype' in data_info:
                if data_info['datatype'] == 'entity' and 'val' in data_info:
                    val = data_info['val']
                    if isinstance(val, str) and 'shm_packet_' in val:
                        has_shm_input = True
                        break
        
        # 如果没有共享内存输入，检查是否是真正的第一个block
        if not has_shm_input:
            # 检查输入数据是否来自全局输入（第一个block的特征）
            is_first_block = False
            has_redis_data = False
            
            for data_name, data_info in input_datas.items():
                if isinstance(data_info, dict):
                    # 检查 from_template_name
                    if 'from_template_name' in data_info:
                        if data_info['from_template_name'] in ['global_inputs', '$USER']:
                            is_first_block = True
                            break
                    # 检查 datatype 为 redis_data_ready 表示来自其他block
                    elif 'datatype' in data_info and data_info['datatype'] == 'redis_data_ready':
                        # 这是来自其他block的数据，不是第一个block
                        has_redis_data = True
                        break
            
            # 如果有redis数据，说明是后续block
            if has_redis_data:
                print(f"后续block（有redis数据），回退到HTTP模式")
                return False
            elif is_first_block:
                print(f"第一个block，强制使用共享内存模式")
                return True
            else:
                print(f"后续block但无共享内存输入，可能是数据传输问题，回退到HTTP模式")
                return False
        
        # 计算输入数据的总大小
        total_size = 0
        for data_name, data_info in input_datas.items():
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
    

    def send_data_remote(self, remote_addr, request_id, workflow_name, template_name, block_name, datas, from_virtual):
        remote_url = 'http://{}:8000/transfer_data'.format(remote_addr)
        data = {'request_id': request_id,
                'workflow_name': workflow_name,
                'template_name': template_name,
                'block_name': block_name,
                'datas': datas,
                'from_virtual': from_virtual}
        requests.post(remote_url, json=data)
    
    
    def receive_data_via_shm(self, packet_id: int) -> Optional[tuple]:
        """通过共享内存接收数据"""
        if not self.shm_enabled or not self.shm_manager:
            return None
        
        try:
            result = self.shm_manager.retrieve_workflow_data(packet_id)
            if result:
                descriptor, data = result
                return descriptor, data
            return None
        except Exception as e:
            print(f"通过共享内存接收数据失败: {e}")
            return None
    
    def free_shm_packet(self, packet_id: int) -> bool:
        """释放共享内存包"""
        if not self.shm_enabled or not self.shm_manager:
            return False
        
        try:
            return self.shm_manager.free_packet(packet_id)
        except Exception as e:
            print(f"释放共享内存包失败: {e}")
            return False
    
    def should_use_shm(self, data_size: int) -> bool:
        """判断是否应该使用共享内存传输"""
        if not self.shm_enabled:
            return False
        
        # 如果数据大小超过阈值，使用共享内存
        return data_size >= config.SHM_THRESHOLD_SIZE

    # def check_runnable(self, state: WorkflowState, workflow_name, function_name):
    #     return state.function_predecessors_cnt[function_name] == len(
    #         self.workflows_info[workflow_name].functions_predecessors[function_name])

    # def send_data_to_container(self, request_id, workflow_name, function_name, datas, datatype):
    #     self.function_manager.send_data(request_id, workflow_name, function_name, datas, datatype)
    #     state = self.workflows_state[request_id]
    #     state.lock.acquire()
    #     state.function_predecessors_cnt[function_name] += len(datas)
    #     if state.function_predecessors_cnt[function_name] == len(
    #             self.workflows_info[workflow_name].functions_predecessors[function_name]):
    #         state.lock.release()
    #         self.trigger_function_local(request_id, workflow_name, function_name)
    #     else:
    #         state.lock.release()
    #
    # def allocate_container(self, request_id, workflow_name, function_name):
    #     state = self.workflows_state[request_id]
    #     state.lock.acquire()
    #     if state.containers_state[function_name] == 'NA':
    #         state.containers_state[function_name] = 'ALLOCATING'
    #         state.lock.release()
    #     else:
    #         state.lock.release()
    #         return
    #     print('allocating', request_id, workflow_name, function_name)
    #     self.function_manager.allocate(request_id, workflow_name, function_name,
    #                                    self.workflows_info[workflow_name].functions_infos[function_name])
    # if state.function_predecessors_cnt[function_name] == len(
    #         self.workflows_info[workflow_name].functions_predecessors[function_name]):
    #     state.lock.acquire()
    #     if function_name not in state.function_executed:
    #         state.function_executed.add(function_name)
    #         state.lock.release()
    #         self.run_function(request_id, workflow_name, function_name)
    #     else:
    #         state.lock.release()

    # def run_block(self, request_id, workflow_name, template_name, block_name):
    #     self.run_normal(request_id, workflow_name, template_name, block_name)

    # def run_normal(self, request_id, workflow_name, template_name, block_name):
    #     self.function_manager.run(request_id, workflow_name, template_name, block_name,
    #                               self.workflows_state[request_id].templates_blocks_inputDatas[template_name][
    #                                   block_name])
