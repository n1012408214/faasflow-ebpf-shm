#!/usr/bin/env python3
"""
测试SVD函数的完整集成流程
"""

import requests
import json
import time

def test_svd_workflow():
    """测试SVD工作流的完整流程"""
    print("=== 测试SVD工作流集成 ===")
    
    # 宿主机WorkerSP地址
    workersp_url = "http://192.168.2.155:8000"
    
    # 测试参数 - 初始化工作流
    init_data = {
        'request_id': 'test_svd_integration_001',
        'workflow_name': 'svd',
        'templates_info': {
            'svd__start': {'ip': '127.0.0.1'},
            'svd__compute': {'ip': '127.0.0.1'},
            'svd__merge': {'ip': '127.0.0.1'}
        }
    }
    
    # 全局输入数据
    global_input_data = {
        'request_id': 'test_svd_integration_001',
        'workflow_name': 'svd',
        'template_name': '$USER',
        'block_name': 'start',
        'datas': {
            'start': {
                'datatype': 'json',
                'val': 'test_data'
            }
        }
    }
    
    try:
        print("1. 初始化SVD工作流...")
        start_time = time.time()
        
        # 初始化工作流
        response = requests.post(
            f"{workersp_url}/request_info",
            json=init_data,
            headers={'Content-Type': 'application/json'},
            timeout=60
        )
        
        if response.status_code != 200:
            print(f"工作流初始化失败: {response.text}")
            return False
        
        print("2. 发送全局输入数据...")
        
        # 发送全局输入数据
        response = requests.post(
            f"{workersp_url}/transfer_data",
            json=global_input_data,
            headers={'Content-Type': 'application/json'},
            timeout=60
        )
        
        end_time = time.time()
        request_time = end_time - start_time
        
        print(f"请求耗时: {request_time:.4f}秒")
        print(f"响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("2. 工作流执行成功")
            print(f"结果: {result}")
            return True
        else:
            print(f"工作流执行失败: {response.text}")
            return False
            
    except Exception as e:
        print(f"测试失败: {e}")
        return False

def test_container_direct():
    """直接测试容器"""
    print("\n=== 直接测试容器 ===")
    
    container_url = "http://172.17.0.3:5000"
    
    # 先创建共享内存包
    import sys
    sys.path.append('src/workflow_manager')
    from shm_utils import FaaSFlowShmManager
    from config import config
    
    shm_manager = FaaSFlowShmManager(
        shm_name=config.SHM_NAME,
        shm_size=config.SHM_SIZE
    )
    
    if not shm_manager.init_shm_pool():
        print("共享内存初始化失败")
        return False
    
    # 创建测试数据
    test_input_data = {
        'matrix': {
            'datatype': 'octet',
            'val': b'test_matrix_data',
            'output_type': 'NORMAL'
        }
    }
    
    # 存储到共享内存
    packet_id = shm_manager.store_workflow_data(
        request_id='test_direct_001',
        workflow_name='svd_workflow',
        template_name='svd_template',
        block_name='block_0',
        function_id=1,
        data=str(test_input_data).encode('utf-8')
    )
    
    print(f"创建共享内存包: ID={packet_id}")
    
    # 测试共享内存端点
    test_data = {
        'packet_id': packet_id,
        'request_id': 'test_direct_001',
        'workflow_name': 'svd_workflow',
        'template_name': 'svd_template',
        'block_name': 'block_0',
        'block_infos': {
            'type': 'NORMAL',
            'output_datas': {
                'matrix': {'type': 'NORMAL'},
                'save_db': {'type': 'NORMAL'}
            }
        }
    }
    
    try:
        print("1. 测试共享内存端点...")
        response = requests.post(
            f"{container_url}/run_block_shm",
            json=test_data,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        print(f"响应状态码: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"共享内存执行结果: {result}")
            return True
        else:
            print(f"共享内存执行失败: {response.text}")
            return False
            
    except Exception as e:
        print(f"直接测试失败: {e}")
        return False

if __name__ == "__main__":
    print("开始SVD集成测试...")
    
    # 测试容器直接调用
    container_success = test_container_direct()
    
    # 测试完整工作流
    workflow_success = test_svd_workflow()
    
    print("\n=== 测试总结 ===")
    print(f"容器直接测试: {'成功' if container_success else '失败'}")
    print(f"完整工作流测试: {'成功' if workflow_success else '失败'}")
    
    if container_success and workflow_success:
        print("🎉 所有测试都通过！SVD共享内存功能工作正常！")
    elif container_success:
        print("⚠️ 容器工作正常，但工作流有问题")
    elif workflow_success:
        print("⚠️ 工作流工作正常，但容器有问题")
    else:
        print("❌ 所有测试都失败")

