
from gevent import monkey

monkey.patch_all()
import gc
import gevent
import socket
import time
import json
import sys
import io

sys.path.append('../../')
from gevent.pywsgi import WSGIServer
from typing import Dict
from workersp import WorkerSPManager
from config import config
from flask import Flask, request
from src.function_manager.file_controller import file_controller
from src.function_manager.prefetcher import prefetcher
from src.workflow_manager.simple_perf_logger import start_request_timer, end_request_timer, log_event, log_data_transfer

app = Flask(__name__)


class Dispatcher:
    def __init__(self, workflows_info_path: dict, functions_info_path: str):
        self.manager = WorkerSPManager(sys.argv[1], config.GATEWAY_URL,
                                       workflows_info_path, functions_info_path)

    def get_state(self, request_id):
        return self.manager.get_state(request_id)

    # def trigger_function(self, state, function_name):
    #     self.manager.trigger_function(state, function_name)

    def receive_incoming_data(self, request_id, workflow_name, template_name, block_name, datas: dict,
                              from_local: bool, from_virtual=None):
        self.manager.receive_incoming_data(request_id, workflow_name, template_name, block_name, datas, from_local,
                                           from_virtual)

    def receive_incoming_request(self, request_id, workflow_name, templates_info):
        self.manager.init_incoming_request(request_id, workflow_name, templates_info)


#print(config.WORKFLOWS_INFO_PATH)
#print(config.FUNCTIONS_INFO_PATH)
dispatcher = Dispatcher(config.WORKFLOWS_INFO_PATH, config.FUNCTIONS_INFO_PATH)

gc_interval = 20


def regular_clear_gc():
    gevent.spawn_later(gc_interval, regular_clear_gc)
    gc.collect()



def socket_server():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', 5999))
    s.listen(100)
    print("Socket服务器启动，监听端口 5999，使用串行模式处理连接")
    while True:
        try:
            c, addr = s.accept()
            # 循环接收所有数据，直到连接关闭
            client_data = b''
            while True:
                chunk = c.recv(64 * 1024)
                if not chunk:
                    break
                client_data += chunk
            c.close()
            
            # 解析并处理数据
            if client_data:
                data = json.loads(client_data)
                dispatcher.receive_incoming_data(data['request_id'], data['workflow_name'], data['template_name'],
                                                 data['block_name'], data['datas'], from_local=True)
        except json.JSONDecodeError as e:
            print(f"Socket连接 {addr} JSON解析失败: {e}")
            print(f"接收到的数据长度: {len(client_data) if 'client_data' in locals() else 0}")
        except Exception as e:
            print(f"Socket连接 {addr} 处理异常: {e}")
            import traceback
            traceback.print_exc()



@app.route('/commit_inter_data', methods=['POST'])
def handle_inter_data_commit():
    data = request.get_json(force=True, silent=True)
    # print('---')
    # print('get inter_data from {} {} {} {}'.format(data['block_name'], data['template_name'], data['workflow_name'],
    #                                                data['request_id']))
    # print(data['datas'].keys(), 'time cost during transmit: ', time.time() - data['post_time'])
    # for k, v in data['datas'].items():
    #     if 'debug' in k:
    #         print(k, v)
    # print()
    dispatcher.receive_incoming_data(data['request_id'], data['workflow_name'], data['template_name'],
                                     data['block_name'], data['datas'], from_local=True)
    return 'OK', 200


@app.route('/transfer_data', methods=['POST'])
def transfer_data():
    try:
        data = request.get_json(force=True, silent=True)
        #print(f"收到transfer_data请求: {data}")
        from_virtual = None
        if 'from_virtual' in data:
            from_virtual = data['from_virtual']
        
        # 记录数据传输开始
        request_id = data['request_id']
        workflow_name = data['workflow_name']
        template_name = data['template_name']
        data_size = len(str(data['datas']).encode('utf-8'))  # 估算数据大小
        
        start_time = time.time()
        log_event(request_id, 'DATA_TRANSFER_START', template_name, None, None, data_size)
        
        dispatcher.receive_incoming_data(data['request_id'], data['workflow_name'], data['template_name'],
                                         data['block_name'], data['datas'], from_local=False, from_virtual=from_virtual)
        
        # 记录数据传输结束和耗时
        end_time = time.time()
        transfer_time = end_time - start_time
        log_data_transfer(request_id, template_name, data_size, transfer_time)
        
        #print(f"transfer_data处理完成: {data['request_id']}")
        return json.dumps({'status': 'ok'})
    except Exception as e:
        print(f"transfer_data处理异常: {e}")
        return json.dumps({'status': 'error', 'message': str(e)}), 500


@app.route('/request_info', methods=['POST'])
def req():
    try:
        data = request.get_json(force=True, silent=True)
        #print(f"收到request_info请求: {data}")
        request_id = data['request_id']
        workflow_name = data['workflow_name']
        templates_info = data['templates_info']
        
        # 开始请求计时
        start_request_timer(request_id, workflow_name)
        log_event(request_id, 'REQUEST_INFO_RECEIVED', workflow_name, None, None, None)
        
        dispatcher.receive_incoming_request(request_id, workflow_name, templates_info)
        
        # 记录请求结束
        end_request_timer(request_id)
        
        #print(f"request_info处理完成: {request_id}")
        return json.dumps({'status': 'ok'})
    except Exception as e:
        print(f"request_info处理异常: {e}")
        return json.dumps({'status': 'error', 'message': str(e)}), 500


@app.route('/test_send_data', methods=['POST'])
def test_send_data():
    data = request.get_json(force=True, silent=True)
    print(type(data))
    return json.dumps({'status': 'ok'})


@app.route('/clear', methods=['POST'])
def clear():
    file_controller.init(config.FILE_CONTROLLER_PATH)
    prefetcher.init(config.PREFETCH_POOL_PATH)
    global dispatcher
    #dispatcher = Dispatcher(config.WORKFLOWS_INFO_PATH, config.FUNCTIONS_INFO_PATH)
    # 移除sleep，让gateway能够立即继续
    return json.dumps({'status': 'ok'})


@app.route('/finish', methods=['POST'])
def finish():
    dispatcher.manager.flow_monitor.upload_all_logs()
    return 'OK', 200


if __name__ == '__main__':
    gc.disable()
    gevent.spawn_later(gc_interval, regular_clear_gc)
    file_controller.init(config.FILE_CONTROLLER_PATH)
    prefetcher.init(config.PREFETCH_POOL_PATH)
    gevent.spawn(socket_server)
    server = WSGIServer(('0.0.0.0', int(sys.argv[2])), app, log=None)
    server.serve_forever()
