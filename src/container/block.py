import gc
import os
import shutil
import sys
import threading
import time
import couchdb
import os

import redis as redis

from store import Store
import container_config

# 添加性能记录支持
try:
    from simple_perf_logger import log_event, log_model_load, log_inference, log_block_execution
    PERF_LOGGING_AVAILABLE = True
except ImportError as e:
    PERF_LOGGING_AVAILABLE = False
    print(f"警告: 性能记录模块不可用: {e}")

source_file = 'main.py'
db = couchdb.Server(container_config.COUCHDB_URL)['results']
latency_db = couchdb.Server(container_config.COUCHDB_URL)['workflow_latency']
redis_db = redis.StrictRedis(host=container_config.REDIS_HOST, port=container_config.REDIS_PORT,
                             db=container_config.REDIS_DB)
shm_mode = container_config.ENABLE_SHARED_MEMORY                             


class Block:
    def __init__(self, block_name, block_path):
        self.block_name = block_name
        self.code = None
        self.cpu = None
        # os.chdir(block_path)
        source_path = os.path.join(block_path, source_file)
        with open(source_path, 'r') as f:
            self.code = compile(f.read(), source_path, mode='exec')

    @classmethod
    def parse(cls, config_dict: dict):
        blocks = {}
        for block_name, block_path in config_dict.items():
            blocks[block_name] = cls(block_name, block_path)
        return blocks

    def run(self, request_id, workflow_name, template_name, templates_infos, block_name, block_inputs: dict,
            block_infos, work_dir, chunk_size, store_queue, custom_store=None):
        st = time.time()
        assert self.code is not None
        os.mkdir(work_dir)
        # print(request_id, block_name, work_dir, file=sys.stderr)
        
        # 记录代码执行开始
        start_time = time.time()
        if PERF_LOGGING_AVAILABLE:
            log_event(request_id, 'BLOCK_CODE_EXECUTE', template_name, block_name, None, None)
        
        # 使用自定义Store或创建新的Store
        if custom_store is not None:
            store = custom_store
        else:
            print('block_inputs:'+str(block_inputs))
            # 检查是否启用共享内存模式
            store = Store(request_id, workflow_name, template_name, templates_infos, block_name, block_inputs, block_infos,
                          chunk_size, store_queue, db, latency_db, redis_db, shm_mode=shm_mode)
        
        ctx = {'request_id': request_id,
               'workflow_name': workflow_name,
               'template_name': template_name,
               'store': store,
               'ENV_WORKDIR': work_dir}

        exec(self.code, ctx)
        
        # 记录代码执行结束和耗时
        end_time = time.time()
        execution_time = end_time - start_time
        if PERF_LOGGING_AVAILABLE:
            log_block_execution(request_id, template_name, block_name, execution_time)
        # thread_ = threading.Thread(target=exec, args=(self.code, self.ctx))
        # thread_.start()
        # thread_.join()

        # print(request_id, workflow_name, template_name, block_name, ed - st, file=sys.stderr)
        foreach_start = False
        parallel_cnt = None
        for k, v in block_infos['output_datas'].items():
            if v['type'] == 'FOREACH':
                #print('VIRTUAL.CNT:'+str(store.outputs_serial[k]), file=sys.stderr)
                foreach_start = True
                parallel_cnt = store.outputs_serial[k]
                #print('parallel_cnt:'+str(parallel_cnt), file=sys.stderr)
                break
        if foreach_start:
            print('post VIRTUAL.CNT:'+str(parallel_cnt), file=sys.stderr)
            store.post('VIRTUAL.CNT', parallel_cnt)
        for t in store.posting_threads:
            t.join()
        shutil.rmtree(work_dir)
        ed = time.time()
        exec_time = ed - st
        transfer_time = store.bypass_size / (0.8 * 50 * 1024 * 1024 * self.cpu)
        return max(0, transfer_time - exec_time)
        # st = time.time()
        # gc.collect()
        # ed = time.time()
        # store.post('gc_time', {'st': st, 'ed': ed}, debug=True)
        # print(self.ctx['store'].cnt, file=sys.stderr)
