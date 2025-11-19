#!/usr/bin/env python3
"""
测试SVD函数的共享内存功能
"""

import requests
import json
import time

def test_svd_shared_memory():
    """测试SVD函数的共享内存功能"""
    print("=== 测试SVD函数共享内存功能 ===")
    
    # 容器地址
    container_url = "http://localhost:5000"
    
    # 测试参数
    test_data = {
        'packet_id': 1,  # 虚拟包ID，实际应该从宿主机传入
        'request_id': 'test_svd_001',
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
        print("1. 发送共享内存块执行请求...")
        start_time = time.time()
        
        response = requests.post(
            f"{container_url}/run_block_shm",
            json=test_data,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        end_time = time.time()
        request_time = end_time - start_time
        
        print(f"请求耗时: {request_time:.4f}秒")
        print(f"响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("2. 解析响应结果...")
            print(f"执行状态: {result.get('status')}")
            print(f"执行时间: {result.get('execution_time', 0):.4f}秒")
            print(f"包数量: {result.get('packet_count', 0)}")
            
            packets = result.get('packets', [])
            if packets:
                print("3. 包描述符详情:")
                for i, packet in enumerate(packets):
                    print(f"  包 {i+1}:")
                    print(f"    包ID: {packet.get('packet_id')}")
                    print(f"    键名: {packet.get('key')}")
                    print(f"    数据类型: {packet.get('datatype')}")
                    print(f"    大小: {packet.get('size', 0)} bytes")
                    print(f"    序列号: {packet.get('serial_num')}")
            else:
                print("3. 未生成任何包")
            
            print("4. 测试完成")
            return True
        else:
            print(f"请求失败: {response.text}")
            return False
            
    except Exception as e:
        print(f"测试失败: {e}")
        return False

def test_traditional_svd():
    """测试传统SVD函数"""
    print("\n=== 测试传统SVD函数 ===")
    
    container_url = "http://localhost:5000"
    
    test_data = {
        'request_id': 'test_svd_traditional_001',
        'workflow_name': 'svd_workflow',
        'template_name': 'svd_template',
        'templates_infos': {},
        'block_name': 'block_0',
        'block_inputs': {},
        'block_infos': {
            'type': 'NORMAL',
            'output_datas': {
                'matrix': {'type': 'NORMAL'},
                'save_db': {'type': 'NORMAL'}
            }
        },
        'chunk_size': 1024
    }
    
    try:
        print("1. 发送传统块执行请求...")
        start_time = time.time()
        
        response = requests.post(
            f"{container_url}/run_block",
            json=test_data,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        end_time = time.time()
        request_time = end_time - start_time
        
        print(f"请求耗时: {request_time:.4f}秒")
        print(f"响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("2. 解析响应结果...")
            print(f"执行时间: {result.get('duration', 0):.4f}秒")
            print(f"延迟时间: {result.get('delay_time', 0):.4f}秒")
            print("3. 传统模式测试完成")
            return True
        else:
            print(f"请求失败: {response.text}")
            return False
            
    except Exception as e:
        print(f"测试失败: {e}")
        return False

if __name__ == "__main__":
    print("开始SVD函数共享内存测试...")
    
    # 测试共享内存模式
    shm_success = test_svd_shared_memory()
    
    # 测试传统模式
    traditional_success = test_traditional_svd()
    
    print("\n=== 测试总结 ===")
    print(f"共享内存模式: {'成功' if shm_success else '失败'}")
    print(f"传统模式: {'成功' if traditional_success else '失败'}")
    
    if shm_success and traditional_success:
        print("两种模式都工作正常！")
    elif shm_success:
        print("共享内存模式工作正常，传统模式有问题")
    elif traditional_success:
        print("传统模式工作正常，共享内存模式有问题")
    else:
        print("两种模式都有问题")



