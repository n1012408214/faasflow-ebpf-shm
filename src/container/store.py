import json
import math
import os
import os.path
import socket
import sys
import time
import threading
import couchdb
import redis
import requests
import logging
import container_config

logger = logging.getLogger(__name__)

# 全局SHM管理器（延迟初始化）
_global_shm_manager = None
_shm_manager_type = None  # 'optimized' 或 'legacy'

def get_shm_manager():
    """获取全局SHM管理器（根据配置选择优化版或旧版）"""
    global _global_shm_manager, _shm_manager_type
    
    if _global_shm_manager is None:
        # 读取配置
        use_optimized = getattr(container_config, 'USE_OPTIMIZED_SHM', True)
        
        print(f"[容器SHM初始化] USE_OPTIMIZED_SHM={use_optimized}")
        
        if use_optimized:
            # 使用优化的SHM方案（idle队列+哈希表）
            try:
                from optimized_container_shm import OptimizedContainerShmUtils
                shm_name = getattr(container_config, 'OPTIMIZED_SHM_NAME', 'faasflow_shm_opt')
                print(f"[容器SHM初始化] 尝试连接优化SHM: {shm_name}")
                
                _global_shm_manager = OptimizedContainerShmUtils(shm_name=shm_name)
                if _global_shm_manager.init_connection():
                    _shm_manager_type = 'optimized'
                    print(f"✓ 容器使用优化SHM方案: {shm_name}")
                    logger.info(f"✓ 使用优化SHM方案: {shm_name}")
                else:
                    print(f"✗ 优化SHM连接失败，尝试旧版SHM")
                    logger.warning("优化SHM连接失败，尝试旧版SHM")
                    _global_shm_manager = None
            except Exception as e:
                print(f"✗ 初始化优化SHM异常: {e}")
                logger.warning(f"初始化优化SHM失败: {e}，尝试旧版SHM")
                import traceback
                traceback.print_exc()
                _global_shm_manager = None
        
        # 回退到旧的Socket-based SHM方案
        if _global_shm_manager is None:
            print(f"[容器SHM初始化] 尝试连接旧版SHM...")
            try:
                from shm_utils import get_global_faasflow_shm_manager
                legacy_manager = get_global_faasflow_shm_manager()
                if legacy_manager and legacy_manager.init_connection():
                    _global_shm_manager = legacy_manager
                    _shm_manager_type = 'legacy'
                    print(f"✓ 容器使用旧版Socket-based SHM")
                    logger.info("✓ 使用旧版Socket-based SHM方案")
                else:
                    print(f"✗ 旧版SHM连接失败，SHM不可用")
                    logger.warning("旧版SHM连接失败，SHM不可用")
                    _global_shm_manager = None
            except Exception as e:
                print(f"✗ 初始化旧版SHM异常: {e}")
                logger.warning(f"初始化旧版SHM失败: {e}，SHM不可用")
                import traceback
                traceback.print_exc()
                _global_shm_manager = None
    
    return _global_shm_manager

def get_shm_manager_type():
    """获取当前SHM管理器类型"""
    return _shm_manager_type

host_url = 'http://172.17.0.1:8000/{}'
disk_reader_url = 'http://172.17.0.1:8001/{}'
db_threshold = 1024 * 16  # >16KB data will be sent to remote db
prefetch_path = '/proxy/mnt'


# Todo
#  Need a translator between function's predefined key_name and workflow's key_name!

class Store:
    def __init__(self, request_id, workflow_name, template_name, templates_infos, block_name, block_inputs: dict,
                 block_infos, chunk_size, store_queue, db, latency_db, redis_db, shm_mode=False):
        # self.cnt = 0
        self.db = db
        self.latency_db = latency_db
        self.redis = redis_db
        self.request_id = request_id
        self.workflow_name = workflow_name
        self.template_name = template_name
        self.templates_infos = templates_infos
        self.block_name = block_name
        self.block_inputs = block_inputs
        self.block_infos = block_infos
        self.chunk_size = chunk_size
        self.store_queue = store_queue
        self.block_outputs = {}
        self.outputs_type = {}
        self.fetch_dict = {}
        self.switch_status = 'NA'
        self.bypass_size = 0
        if self.block_infos['type'] == 'SWITCH':
            self.switch_status = 'PENDING'
        self.switch_branch = None
        self.block_serial = None
        for k, v in self.block_inputs.items():
            if 'output_type' in v and v['output_type'] == 'FOREACH':
                self.block_serial = v['serial_num']
        self.posting_threads = []
        self.outputs_serial = {}
        
        # 共享内存模式支持
        self.shm_mode = shm_mode
        raw_port = getattr(container_config, 'CONTAINER_PORT', 0)
        try:
            self.container_port = int(raw_port)
        except (TypeError, ValueError):
            self.container_port = 0
        if self.shm_mode:
            print(f"[SHM] 当前容器端口: {self.container_port}")

    def fetch_scalability_config(self):
        try:
            data = self.db['scalability_config']
        except Exception:
            return None
        return data

    def post_to_disk(self, key, val, datatype, serial_num):
        assert datatype == 'json' or datatype == 'octet'
        filename = self.request_id + '.' + self.generate_db_key(key, serial_num)
        if datatype == 'octet':
            filepath = os.path.join(prefetch_path, filename)
            with open(filepath, 'wb') as f:
                f.write(val)
        elif datatype == 'json':
            filepath = os.path.join(prefetch_path, filename + '.json')
            with open(filepath, 'w') as f:
                f.write(val)
        else:
            raise Exception

    def generate_db_key(self, key, serial_num):
        return self.request_id + '.' + self.template_name + '.' + self.block_name + '.out.' + key + '.' + str(
            serial_num)

    def put_bigdata(self, key, val, datatype, serial_num):
        # Todo: potential problem: if couchdb is too slow, then the expired redis data will be uploaded to couchdb while
        #  this store is uploading to couchdb
        st = time.time()
        ips_cnt, local_cnt, remote_cnt, final_cnt = self.get_destination_locality(key)
        ed = time.time()
        # print('get_destination info', ed - st, file=sys.stderr)
        db_key = self.generate_db_key(key, serial_num)
        if datatype == 'json':
            db_key += '.json'
        if local_cnt > 0 or final_cnt > 0:
            # print('begin_put_to_redis', time.time() - start, file=sys.stderr)
            # st = time.time()
            self.put_to_redis(key, db_key, val, datatype, serial_num, local_cnt, remote_cnt)
            # ed = time.time()
            # print('put_to_redis', ed - st, file=sys.stderr)
            # t = threading.Thread(target=self.put_to_redis,
            #                      args=(key, db_key, val, datatype, serial_num, local_cnt, remote_cnt))
            # self.posting_threads.append(t)
            # t.start()
            # t.join()
        if remote_cnt > 0:
            # tmp = json.dumps({'request_id': self.request_id,
            #                   'workflow_name': self.workflow_name,
            #                   'template_name': self.template_name,
            #                   'block_name': self.block_name,
            #                   'key': key,
            #                   'db_key': db_key,
            #                   'datasize': len(val),
            #                   'datatype': datatype,
            #                   'switch_branch': self.switch_branch,
            #                   'serial_num': serial_num,
            #                   'output_type': self.block_infos['output_datas'][key]['type'],
            #                   'ips_cnt': ips_cnt,
            #                   'post_time': time.time()})
            # st = time.time()
            # requests.post('http://127.0.0.1:5000/post_data', data=val, headers={'json': tmp})
            # ed = time.time()
            # print('post_to_bypass_store time: ', ed - st, file=sys.stderr)
            self.store_queue.put({'request_id': self.request_id,
                                  'workflow_name': self.workflow_name,
                                  'template_name': self.template_name,
                                  'block_name': self.block_name,
                                  'key': key,
                                  'db_key': db_key,
                                  'val': val,
                                  'datasize': len(val),
                                  'datatype': datatype,
                                  'switch_branch': self.switch_branch,
                                  'serial_num': serial_num,
                                  'output_type': self.block_infos['output_datas'][key]['type'],
                                  'ips_cnt': ips_cnt,
                                  'post_time': time.time()})
            self.bypass_size += len(val)

    def put_to_redis(self, key, db_key, val, datatype, serial_num, local_cnt, remote_cnt):
        assert datatype == 'json' or datatype == 'octet'
        # st = time.time()
        self.redis[db_key] = val
        # ed = time.time()
        # print('redis post time consuming:', db_key, len(val), ed - st)
        # print('begin_post_to_host', time.time() - start, file=sys.stderr)
        # st = time.time()
        # t = threading.Thread(target=self.post_redis_data_ready_to_host,
        #                      args=(key, db_key, len(val), serial_num, local_cnt, remote_cnt))
        # self.posting_threads.append(t)
        # t.start()
        self.post_redis_data_ready_to_host(key, db_key, len(val), serial_num, local_cnt, remote_cnt)
        # ed = time.time()
        # print('host post time consuming:', db_key, ed - st)
        # print('ending', time.time() - start, file=sys.stderr)
        # self.redis.expire(db_key, 100)

    def put_to_couch(self, key, db_key, val, datatype, serial_num, ips_cnt):
        assert datatype == 'json' or datatype == 'octet'
        file_size = len(val)
        chunk_num = math.ceil(file_size / self.chunk_size)
        # doc = self.db[self.request_id]
        for i in range(chunk_num):
            chunk_val = val[(i * self.chunk_size):((i + 1) * self.chunk_size)]
            while True:
                try:
                    st = time.time()
                    self.db.put_attachment(self.db[self.request_id], chunk_val, filename=db_key + '.' + str(i),
                                           content_type='application/' + datatype)
                    ed = time.time()
                    # print('couchDB post time consuming:', key, ed - st, file=sys.stderr)
                    break
                except Exception as e:
                    print(e, file=sys.stderr)
                    time.sleep(0.05)
                    pass
            if i == 0:
                self.post_couch_data_ready_to_host(key, db_key, len(val), serial_num, ips_cnt, chunk_num)

    def post_couch_data_ready_to_host(self, key, db_key, size, serial_num, ips_cnt, chunk_num):
        post_data = {'request_id': self.request_id,
                     'workflow_name': self.workflow_name,
                     'template_name': self.template_name,
                     'block_name': self.block_name,
                     'datas': {key: {'datatype': 'couch_data_ready', 'datasize': size, 'db_key': db_key,
                                     'switch_branch': self.switch_branch, 'serial_num': serial_num,
                                     'output_type': self.block_infos['output_datas'][key]['type'],
                                     'ips_cnt': ips_cnt, 'chunk_num': chunk_num}},
                     'post_time': time.time()}
        requests.post(host_url.format('commit_inter_data'), json=post_data)

    def post_metadata_to_host(self, key, size, serial_num):
        post_data = {'request_id': self.request_id,
                     'workflow_name': self.workflow_name,
                     'template_name': self.template_name,
                     'block_name': self.block_name,
                     'datas': {key: {'datatype': 'metadata', 'datasize': size,
                                     'db_key': self.generate_db_key(key, serial_num),
                                     'switch_branch': self.switch_branch, 'serial_num': serial_num,
                                     'output_type': self.block_infos['output_datas'][key]['type']}},
                     'post_time': time.time()}
        requests.post(host_url.format('commit_inter_data'), json=post_data)

    def post_redis_data_ready_to_host(self, key, db_key, size, serial_num, local_cnt, remote_cnt):
        post_data = {'request_id': self.request_id,
                     'workflow_name': self.workflow_name,
                     'template_name': self.template_name,
                     'block_name': self.block_name,
                     'datas': {key: {'datatype': 'redis_data_ready', 'datasize': size, 'db_key': db_key,
                                     'local_cnt': local_cnt, 'remote_cnt': remote_cnt,
                                     'switch_branch': self.switch_branch, 'serial_num': serial_num,
                                     'output_type': self.block_infos['output_datas'][key]['type']}},
                     'post_time': time.time()}
        s = socket.socket()
        s.connect(('172.17.0.1', 5999))
        s.sendall(bytes(json.dumps(post_data), encoding='UTF-8'))
        s.close()
        # st = time.time()
        # requests.post(host_url.format('commit_inter_data'), json=post_data)
        # ed = time.time()
        # print('host post time consuming:', db_key, ed - st, file=sys.stderr)

    def post_data_fetched_to_host(self, db_key):
        # Todo: this message can be batched
        post_data = {'request_id': self.request_id,
                     'workflow_name': self.workflow_name,
                     'template_name': self.template_name,
                     'block_name': self.block_name,
                     'datas': {db_key: {'datatype': 'data_fetched', 'db_key': db_key}},
                     'post_time': time.time()}
        s = socket.socket()
        s.connect(('172.17.0.1', 5999))
        s.sendall(bytes(json.dumps(post_data), encoding='UTF-8'))
        s.close()
        # requests.post(host_url.format('commit_inter_data'), json=post_data)

    def post_direct_to_host(self, k, v, datatype, serial_num):
        if datatype == 'octet':
            v = bytes.decode(v)
            datatype = 'base64'
        post_data = {'request_id': self.request_id,
                     'workflow_name': self.workflow_name,
                     'template_name': self.template_name,
                     'block_name': self.block_name,
                     'datas': {k: {'datatype': datatype, 'val': v, 'switch_branch': self.switch_branch,
                                   'serial_num': serial_num,
                                   'output_type': self.block_infos['output_datas'][k]['type']}},
                     'post_time': time.time()}
        s = socket.socket()
        s.connect(('172.17.0.1', 5999))
        s.sendall(bytes(json.dumps(post_data), encoding='UTF-8'))
        s.close()
        # requests.post(host_url.format('commit_inter_data'), json=post_data)
        # print('put_direct_to_host finished!', k, file=sys.stderr)
    
    def notify_host_shm_data(self, key, shm_id, datatype, serial_num):
        """通知宿主机SHM数据已准备好（复用现有触发机制）"""
        try:
            # 构建SHM数据格式
            # 特殊处理VIRTUAL.CNT
            if key == 'VIRTUAL.CNT':
                output_type = 'NORMAL'  # VIRTUAL.CNT是特殊类型
                data_type = 'NORMAL'
            else:
                output_type = self.block_infos['output_datas'][key]['type']
                data_type = self.block_infos['output_datas'][key]['type']
            
            # 根据SHM类型选择不同的标识符格式
            shm_type = get_shm_manager_type()
            if shm_type == 'optimized':
                # 优化SHM：使用desc_index
                shm_identifier = f'shm_desc_{shm_id}'
                id_type = "描述符索引"
            else:
                # 旧版SHM：使用packet_id
                shm_identifier = f'shm_packet_{shm_id}'
                id_type = "包ID"
            
            post_data = {
                'request_id': self.request_id,
                'workflow_name': self.workflow_name,
                'template_name': self.template_name,
                'block_name': self.block_name,
                'datas': {
                    key: {
                        'datatype': data_type,  # 数据类型
                        'val': shm_identifier,  # SHM标识符
                        'switch_branch': self.switch_branch,
                        'serial_num': serial_num,
                        'output_type': output_type
                    }
                },
                'post_time': time.time()
            }
            print('serial_num:'+str(serial_num))
            # 复用现有的Socket通信逻辑
            s = socket.socket()
            s.connect(('172.17.0.1', 5999))
            s.sendall(bytes(json.dumps(post_data), encoding='UTF-8'))
            s.close()
            
            #print(f"SHM数据通知已发送: {key}, {id_type}={shm_id}")
            
        except Exception as e:
            print(f"发送SHM数据通知失败: {e}")
            # 如果通知失败，回退到传统模式
            self._post_traditional(key, None, datatype, False)

    def handle_switch(self):
        for condition in self.block_infos['conditions']:
            if condition == 'default' or eval(condition, self.block_outputs):
                self.switch_branch = condition
                break
        for key in self.block_infos['conditions'][self.switch_branch]:
            if key.startswith('virtual'):
                self.block_outputs[key] = 'ok'
                self.outputs_type[key] = 'json'
                self.block_infos['output_datas'][key] = {'type': 'NORMAL'}
            self.post(key, self.block_outputs[key], datatype=self.outputs_type[key])

    def is_affinity_possible(self, key):
        # Todo: This result can be pre-calculated!
        dest = None
        if self.block_infos['type'] == 'SWITCH':
            dest = self.block_infos['conditions'][self.switch_branch][key]
        elif self.block_infos['type'] == 'NORMAL':
            dest = self.block_infos['output_datas'][key]['dest']
        for dest_template_name in dest.keys():
            if dest_template_name == self.template_name:
                return True
        return False

    def get_destination_locality(self, key):
        # Todo: This can be pre-calculated
        dest = None
        local_cnt = 0
        remote_cnt = 0
        final_cnt = 0
        ips_cnt = {}
        if self.block_infos['type'] == 'SWITCH':
            dest = self.block_infos['conditions'][self.switch_branch][key]
        elif self.block_infos['type'] == 'NORMAL':
            dest = self.block_infos['output_datas'][key]['dest']
        local_ip = self.templates_infos[self.template_name]['ip']
        for dest_template_name, dest_template_infos in dest.items():
            if dest_template_name == '$USER':
                final_cnt += 1
                continue
            target_ip = self.templates_infos[dest_template_name]['ip']
            if target_ip == local_ip:
                local_cnt += len(dest_template_infos)
            else:
                remote_cnt += len(dest_template_infos)
            if target_ip not in ips_cnt:
                ips_cnt[target_ip] = 0
            ips_cnt[target_ip] += len(dest_template_infos)
        return ips_cnt, local_cnt, remote_cnt, final_cnt

    def _calculate_dependency_count(self, key):
        """计算数据依赖计数（确保至少为1）"""
        try:
            # 获取输出目标信息
            ips_cnt, local_cnt, remote_cnt, final_cnt = self.get_destination_locality(key)
            # 依赖计数 = 本地消费者数量
            # 注意：必须至少为1，否则会被立即回收
            dependency_count = local_cnt
            
            print(f"[依赖计数] key={key}, local_cnt={local_cnt}, dependency_count={dependency_count}")
            
            return dependency_count
        except Exception as e:
            print(f"计算依赖计数失败: {e}")
            return 1  # 默认依赖计数为1

    def post(self, key, val, force=False, datatype='json', debug=False):
        post_start_time = time.time()
        
        if debug:
            st = val['st']
            ed = val['ed']
            self.latency_db.save({'request_id': self.request_id, 'template_name': self.template_name,
                                  'block_name': self.block_name + f'_{key}', 'phase': 'use_container',
                                  'time': ed - st, 'st': st, 'ed': ed})
            return
        
        # 启动性能日志记录（如果还没有启动）
        try:
            from simple_perf_logger import SimplePerfLogger
            if not hasattr(self, 'perf_logger') or self.perf_logger is None:
                self.perf_logger = SimplePerfLogger()
                self.perf_logger.start_request(self.request_id, self.workflow_name)
            self.PERF_LOGGING_AVAILABLE = True
        except ImportError:
            self.PERF_LOGGING_AVAILABLE = False
            self.perf_logger = None
        
        # 记录数据上传开始
        if self.PERF_LOGGING_AVAILABLE and self.perf_logger:
            self.perf_logger.log_event(self.request_id, 'DATA_UPLOAD_START', self.template_name, f"{self.block_name}.{key}", None, None)
        
        start_time = time.time()
        
        # 共享内存模式处理
        if self.shm_mode:
            result = self._post_to_shm(key, val, datatype, debug)
        else:
            # 传统模式处理
            result = self._post_traditional(key, val, datatype, debug)
        
        end_time = time.time()
        
        # 记录数据上传结束
        if self.PERF_LOGGING_AVAILABLE and self.perf_logger:
            upload_time = end_time - start_time
            data_size = len(str(val)) if val else 0
            self.perf_logger.log_event(self.request_id, 'DATA_UPLOAD_END', self.template_name, f"{self.block_name}.{key}", upload_time, data_size)
        
        # 计算并输出传输时间
        post_total_time = time.time() - post_start_time
        data_size = len(json.dumps(val)) if datatype == 'json' else len(val)
        mode = "SHM" if self.shm_mode else "Redis"
        #print(f"[{mode}传输] POST key={key}, 时间={post_total_time*1000:.2f}ms, 大小={data_size} bytes")
        
        return result
    
    def _post_to_shm(self, key, val, datatype='json', debug=False):
        """优化的共享内存模式数据输出（使用idle队列+哈希表）"""
        # 特殊处理VIRTUAL.CNT：直接通过传统方式发送，不使用SHM
        if key == 'VIRTUAL.CNT':
            # VIRTUAL.CNT的值本身就是计数，使用该值作为序列号
            serial_num = val if isinstance(val, int) else 0
            self.post_direct_to_host(key, val, datatype, serial_num)
            return val
        
        # 处理SWITCH状态
        if self.switch_status == 'PENDING':
            self.block_outputs[key] = val
            self.outputs_type[key] = datatype
            if len(self.block_outputs) == len(self.block_infos['output_datas']):
                self.switch_status = 'READY'
                self.handle_switch()
            return
        
        # 正常的序列号管理（排除特殊变量）
        if key not in self.outputs_serial:
            self.outputs_serial[key] = 0
        else:
            self.outputs_serial[key] += 1
        serial_num = self.outputs_serial[key]
        if self.block_serial is not None:
            serial_num = self.block_serial
        
        # 获取SHM管理器（根据配置自动选择优化版或旧版）
        try:
            shm_manager = get_shm_manager()
            if not shm_manager:
                # SHM不可用，回退到Redis模式
                return self._post_traditional(key, val, datatype, debug)
            
            # 计算依赖计数（基于输出目标数量）
            dependency_count = self._calculate_dependency_count(key)
            
            # 根据SHM类型调用不同的接口
            shm_type = get_shm_manager_type()
            
            if shm_type == 'optimized':
                # 优化SHM：直接使用post_data（已包含分配、写入、状态更新）
                # 返回desc_index而不是packet_id
                shm_id = shm_manager.post_data(
                    key=key,
                    val=val,
                    datatype=datatype,
                    function_id=self.container_port,
                    dependency_count=dependency_count
                )
            elif shm_type == 'legacy':
                # 旧版SHM：使用post_data_global（Socket-based）
                # 返回packet_id
                shm_id = shm_manager.post_data(
                    key=key,
                    val=val,
                    datatype=datatype,
                    function_id=self.container_port,
                    dependency_count=dependency_count
                )
            else:
                logger.error(f"未知的SHM类型: {shm_type}")
                return self._post_traditional(key, val, datatype, debug)
            
            if shm_id:
                # 发送SHM数据通知到宿主机（触发下游函数）
                # 注意：
                # - 优化SHM：shm_id是desc_index，宿主机不管理描述符，但仍需通知以触发workflow
                # - 旧版SHM：shm_id是packet_id，宿主机管理描述符，需要通知
                self.notify_host_shm_data(key, shm_id, datatype, serial_num)
                return shm_id
            else:
                # 回退到Redis模式
                logger.warning(f"SHM输出失败，回退到Redis模式: key={key}")
                return self._post_traditional(key, val, datatype, debug)
            
        except Exception as e:
            logger.error(f"SHM输出异常: {e}，回退到Redis模式")
            import traceback
            traceback.print_exc()
            # 回退到Redis模式
            return self._post_traditional(key, val, datatype, debug)
    
    def _post_traditional(self, key, val, datatype='json', debug=False):
        """传统模式的数据输出"""
        assert datatype == 'json' or datatype == 'octet'
        if key not in self.outputs_serial:
            self.outputs_serial[key] = 0
        else:
            self.outputs_serial[key] += 1
        serial_num = self.outputs_serial[key]
        if self.block_serial is not None:
            serial_num = self.block_serial
        # Note: A switch block can't handle foreach output!!!
        if self.switch_status == 'PENDING':
            self.block_outputs[key] = val
            self.outputs_type[key] = datatype
            if len(self.block_outputs) == len(self.block_infos['output_datas']):
                self.switch_status = 'READY'
                self.handle_switch()
            return
        # print('POST', key, file=sys.stderr)
        val_db = val
        if datatype == 'json':
            val_db = json.dumps(val)
        size = len(val_db)
        if size > db_threshold or datatype == 'octet':
            # print('enter_put_bitdata', time.time() - start, file=sys.stderr)
            self.put_bigdata(key, val_db, datatype, serial_num)
        else:
            self.post_direct_to_host(key, val, datatype, serial_num)
            # t = threading.Thread(target=self.post_direct_to_host, args=(key, val, datatype, serial_num))
            # self.posting_threads.append(t)
            # t.start()

    def fetch(self, keys):
        fetch_start_time = time.time()
        self.fetch_dict = {}
        threads = []
        for k in keys:
            key_start_time = time.time()
            self.get_input_data(k, self.block_infos['input_datas'][k]['type'])
            key_end_time = time.time()
            mode = "SHM" if self.shm_mode else "Redis"
            #print(f"[{mode}传输] FETCH key={k}, 时间={(key_end_time - key_start_time)*1000:.2f}ms")
            # threads.append(
            #     threading.Thread(target=self.get_input_data, args=(k, self.block_infos['input_datas'][k]['type'])))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        fetch_total_time = time.time() - fetch_start_time
        mode = "SHM" if self.shm_mode else "Redis"
        #print(f"[{mode}传输] FETCH 总时间={fetch_total_time*1000:.2f}ms, keys={list(self.fetch_dict.keys())}")
        return self.fetch_dict

    def get_input_data(self, key, datatype='NORMAL'):
        if key not in self.block_inputs:
            raise Exception('no such input_data: ', key)
        
        # 启动性能日志记录（如果还没有启动）
        try:
            from simple_perf_logger import SimplePerfLogger
            if not hasattr(self, 'perf_logger') or self.perf_logger is None:
                self.perf_logger = SimplePerfLogger()
                self.perf_logger.start_request(self.request_id, self.workflow_name)
            self.PERF_LOGGING_AVAILABLE = True
        except ImportError:
            self.PERF_LOGGING_AVAILABLE = False
            self.perf_logger = None
        
        if datatype == 'NORMAL':
            # 记录数据获取开始
            if self.PERF_LOGGING_AVAILABLE and self.perf_logger:
                self.perf_logger.log_event(self.request_id, 'DATA_FETCH_START', self.template_name, key, None, None)
            
            start_time = time.time()
            self.fetch_dict[key] = self.fetch_input_data(key, self.block_inputs[key])
            end_time = time.time()
            
            # 记录数据获取结束
            if self.PERF_LOGGING_AVAILABLE and self.perf_logger:
                fetch_time = end_time - start_time
                data_size = len(str(self.fetch_dict[key])) if self.fetch_dict[key] else 0
                self.perf_logger.log_event(self.request_id, 'DATA_FETCH_END', self.template_name, key, fetch_time, data_size)
                
        elif datatype == 'LIST':
            # This data is a merged list after foreach
            self.fetch_dict[key] = []
            
            # 记录列表数据获取开始
            #if self.PERF_LOGGING_AVAILABLE and self.perf_logger:
            #    self.perf_logger.log_event(self.request_id, 'DATA_FETCH_LIST_START', self.template_name, key, None, None)
            #
            #total_start_time = time.time()
            #total_data_size = 0

            # 调试：打印该列表输入可用的键及其类型
            try:
                available_keys = list(self.block_inputs[key].keys())
                key_types = {repr(k): type(k).__name__ for k in self.block_inputs[key].keys()}
                print(f"[DEBUG][fetch_list] key={key} available_keys={available_keys} key_types={key_types}")
            except Exception as e:
                print(f"[DEBUG][fetch_list] key={key} keys-introspection-failed: {e}")
            
            # 直接遍历实际存在的键，按数字顺序排序
            sorted_keys = sorted(self.block_inputs[key].keys(), key=lambda x: int(x) if x.isdigit() else float('inf'))
            for index_key in sorted_keys:
                # 记录单个列表项获取开始
                if self.PERF_LOGGING_AVAILABLE and self.perf_logger:
                    self.perf_logger.log_event(self.request_id, 'DATA_FETCH_ITEM_START', self.template_name, f"{key}[{index_key}]", None, None)
                
                # 调试信息
                print(f"[DEBUG][fetch_list] index_key={index_key}, value={self.block_inputs[key][index_key]}")

                item_start_time = time.time()
                item_data = self.fetch_input_data(key, self.block_inputs[key][index_key])
                item_end_time = time.time()
                
                self.fetch_dict[key].append(item_data)
                
                # 记录单个列表项获取结束
                #if self.PERF_LOGGING_AVAILABLE and self.perf_logger:
                #    item_fetch_time = item_end_time - item_start_time
                #    item_data_size = len(str(item_data)) if item_data else 0
                #    total_data_size += item_data_size
                #    self.perf_logger.log_event(self.request_id, 'DATA_FETCH_ITEM_END', self.template_name, f"{key}[{i}]", item_fetch_time, item_data_size)
            
            total_end_time = time.time()
            
            # 记录列表数据获取结束
            #if self.PERF_LOGGING_AVAILABLE and self.perf_logger:
            #    total_fetch_time = total_end_time - total_start_time
            #    self.perf_logger.log_event(self.request_id, 'DATA_FETCH_LIST_END', self.template_name, key, total_fetch_time, total_data_size)
        else:
            raise Exception

    def fetch_input_data(self, key, data_infos):
        datatype = data_infos['datatype']
        
        # 首先检查是否是SHM数据（优先级最高）
        val = data_infos.get('val', '')
        if isinstance(val, str) and 'shm_desc_' in val:
            # 处理共享内存描述符索引（任何数据类型都可能是SHM数据）
            return self.fetch_from_shm(data_infos)
        elif datatype == 'redis_data_ready':
            return self.fetch_from_redis(data_infos['db_key'])
        elif datatype == 'couch_data_ready':
            return self.fetch_from_couch(data_infos['db_key'])
        elif datatype == 'json' or datatype == 'octet':
            return data_infos['val']
        elif datatype == 'disk_data_ready':
            return self.fetch_from_disk(data_infos['db_key'])
        else:
            raise Exception(f"未知数据类型: {datatype}")
    
    def fetch_from_shm(self, data_infos):
        """从共享内存读取数据（根据配置自动选择优化版或旧版）"""
        shm_fetch_start_time = time.time()
        
        if not self.shm_mode:
            raise Exception("共享内存模式未启用")
        
        try:
            shm_manager = get_shm_manager()
            if not shm_manager:
                raise Exception("SHM管理器不可用")
            
            # 从val字段提取描述符索引
            shm_desc_index = int(data_infos['val'].split('_')[-1])
            
            # 获取SHM类型
            shm_type = get_shm_manager_type()
            
            # 输出详细的描述符信息用于调试
            print(f"[FETCH调试] 开始获取desc_index={shm_desc_index}, SHM类型={shm_type}")
            
            # 从共享内存读取数据
            read_start_time = time.time()
            if shm_type == 'optimized':
                # 优化SHM：直接使用fetch_data方法
                data = shm_manager.fetch_data(shm_desc_index)
            else:
                # 旧版SHM：使用fetch_data_by_packet_id方法
                data = shm_manager.fetch_data_by_packet_id(shm_desc_index)
            read_end_time = time.time()
            
            if data:
                # 将bytes数据解码为字符串
                try:
                    decoded_data = data.decode('utf-8')
                    
                    # 尝试JSON反序列化（与传统模式保持一致）
                    try:
                        import json
                        deserialize_start_time = time.time()
                        deserialized_data = json.loads(decoded_data)
                        deserialize_end_time = time.time()
                        
                        shm_total_time = time.time() - shm_fetch_start_time
                        read_time = read_end_time - read_start_time
                        deserialize_time = deserialize_end_time - deserialize_start_time
                        
                        # 只输出关键时间信息，区分SHM类型
                        shm_label = "优化SHM" if shm_type == 'optimized' else "旧版SHM"
                        print(f"[{shm_label}] desc_index={shm_desc_index}, 读取={read_time*1000:.2f}ms, 反序列化={deserialize_time*1000:.2f}ms, 大小={len(data)} bytes")
                        return deserialized_data
                    except json.JSONDecodeError as e:
                        shm_total_time = time.time() - shm_fetch_start_time
                        shm_label = "优化SHM" if shm_type == 'optimized' else "旧版SHM"
                        print(f"[{shm_label}] desc_index={shm_desc_index}, 读取={shm_total_time*1000:.2f}ms, 大小={len(data)} bytes (字符串)")
                        return decoded_data
                        
                except UnicodeDecodeError as e:
                    logger.error(f"SHM数据解码失败: {e}")
                    return data
            else:
                raise Exception(f"SHM数据读取失败: 描述符索引={shm_desc_index}")
        except Exception as e:
            logger.error(f"从SHM读取数据异常: {e}")
            raise

    def fetch_from_disk(self, key):
        st = time.time()
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect('/proxy/mnt/transfer.sock')
        request_info = {'db_key': key}
        s.sendall(bytes(json.dumps(request_info), encoding='UTF-8'))
        data = []
        chunk = s.recv(container_config.SOCKET_CHUNK_SIZE)
        while chunk:
            data.append(chunk)
            chunk = s.recv(container_config.SOCKET_CHUNK_SIZE)
        s.close()
        data = b''.join(data)
        ed = time.time()
        # print('socket fetch from disk', ed - st, file=sys.stderr)
        self.post_data_fetched_to_host(key)
        # r = requests.get(disk_reader_url.format('fetch_from_disk'), json={'db_key': key})
        # t = threading.Thread(target=self.post_data_fetched_to_host, args=(key,))
        # self.posting_threads.append(t)
        # t.start()
        if key[-4:] == 'json':
            return json.loads(data)
        else:
            return data

    def fetch_from_redis(self, key):
        redis_fetch_start_time = time.time()
        # print('fetching_from_redis:', key, file=sys.stderr)
        val = None
        if key[-4:] == 'json':
            try:
                redis_get_start = time.time()
                raw_data = self.redis[key].decode()
                redis_get_end = time.time()
                
                deserialize_start = time.time()
                val = json.loads(raw_data)
                deserialize_end = time.time()
                
                redis_total_time = time.time() - redis_fetch_start_time
                get_time = redis_get_end - redis_get_start
                deserialize_time = deserialize_end - deserialize_start
                print(f"[容器Redis时间] key={key}, 总时间={redis_total_time*1000:.2f}ms (Redis读取={get_time*1000:.2f}ms, 反序列化={deserialize_time*1000:.2f}ms), 大小={len(raw_data)} bytes")
            except Exception as e:
                print(f"[容器Redis错误] 读取失败: {e}")
                pass
        else:
            try:
                redis_get_start = time.time()
                val = self.redis[key]
                redis_get_end = time.time()
                
                redis_total_time = time.time() - redis_fetch_start_time
                get_time = redis_get_end - redis_get_start
                print(f"[容器Redis时间] key={key}, 总时间={redis_total_time*1000:.2f}ms (Redis读取={get_time*1000:.2f}ms), 大小={len(val)} bytes")
            except Exception as e:
                print(f"[容器Redis错误] 读取失败: {e}")
                pass
        if val is not None:
            self.post_data_fetched_to_host(key)
            # t = threading.Thread(target=self.post_data_fetched_to_host, args=(key,))
            # self.posting_threads.append(t)
            # t.start()
            return val
        print('fetch_from_redis failed:', key, file=sys.stderr)
        return self.fetch_from_disk(key)

    def fetch_from_couch(self, key):
        if key[-4:] == 'json':
            return json.loads(self.db.get_attachment(self.request_id, filename=key, default='no attachment').read())
        else:
            return self.db.get_attachment(self.request_id, filename=key, default='no attachment').read()
        # octet_data = self.db.get_attachment(self.request_id, filename=key, default='no attachment')
        # if octet_data != 'no attachment':
        #     print(type(octet_data), file=sys.stderr)
        #     return octet_data.read()
        # else:
        #     return json.loads(self.db.get_attachment(self.request_id, filename=key + '.json', default='no attachment'))
