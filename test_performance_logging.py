#!/usr/bin/env python3
"""
测试性能记录功能
"""

import os
import sys
import time
import json
from datetime import datetime

# 设置环境变量
os.environ['PERF_LOG_FILE'] = '/tmp/faasflow_test_perf.log'

# 添加路径
sys.path.append('src/workflow_manager')
sys.path.append('src/container')

def test_performance_logger():
    """测试性能记录器"""
    print("=== 测试性能记录功能 ===")
    
    try:
        # 导入性能记录模块
        from simple_perf_logger import start_request_timer, end_request_timer, log_event, log_block_execution
        
        # 测试请求
        request_id = "test_request_001"
        workflow_name = "recognizer"
        template_name = "recognizer__adult"
        block_name = "block_0"
        
        print(f"开始测试请求: {request_id}")
        
        # 开始请求计时
        start_request_timer(request_id, workflow_name)
        
        # 模拟一些事件
        log_event(request_id, 'CONTAINER_START', template_name)
        time.sleep(0.1)  # 模拟容器启动时间
        
        log_event(request_id, 'CONTAINER_READY', template_name)
        time.sleep(0.05)  # 模拟容器就绪时间
        
        log_event(request_id, 'BLOCK_EXECUTION_START', template_name, block_name)
        time.sleep(0.2)  # 模拟块执行时间
        
        # 记录块执行结束
        log_block_execution(request_id, template_name, block_name, 0.2)
        
        # 结束请求
        end_request_timer(request_id)
        
        print("✓ 性能记录测试完成")
        
        # 检查日志文件
        log_file = '/tmp/faasflow_test_perf.log'
        if os.path.exists(log_file):
            print(f"✓ 日志文件已生成: {log_file}")
            
            # 读取并显示日志内容
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                print(f"✓ 记录了 {len(lines)} 个事件")
                
                for i, line in enumerate(lines[-5:], 1):  # 显示最后5行
                    try:
                        event = json.loads(line.strip())
                        print(f"  事件{i}: {event['event']} - {event['timestamp']}")
                    except:
                        print(f"  事件{i}: {line.strip()}")
        else:
            print("✗ 日志文件未生成")
            
    except ImportError as e:
        print(f"✗ 导入性能记录模块失败: {e}")
    except Exception as e:
        print(f"✗ 测试失败: {e}")

def test_container_performance_logger():
    """测试容器性能记录器"""
    print("\n=== 测试容器性能记录功能 ===")
    
    try:
        # 导入容器性能记录模块
        from src.container.simple_perf_logger import start_request_timer, end_request_timer, log_event, log_block_execution, log_model_load, log_inference
        
        # 测试请求
        request_id = "test_container_request_001"
        workflow_name = "recognizer"
        template_name = "recognizer__adult"
        block_name = "block_0"
        
        print(f"开始测试容器请求: {request_id}")
        
        # 开始请求计时
        start_request_timer(request_id, workflow_name)
        
        # 模拟模型加载
        log_event(request_id, 'MODEL_LOAD_START', template_name, block_name)
        time.sleep(0.3)  # 模拟模型加载时间
        log_model_load(request_id, template_name, 0.3)
        
        # 模拟推理
        log_event(request_id, 'INFERENCE_START', template_name, block_name)
        time.sleep(0.7)  # 模拟推理时间
        log_inference(request_id, template_name, 0.7)
        
        # 记录块执行
        log_block_execution(request_id, template_name, block_name, 1.0)
        
        # 结束请求
        end_request_timer(request_id)
        
        print("✓ 容器性能记录测试完成")
        
        # 检查日志文件
        log_file = '/tmp/faasflow_container_perf.log'
        if os.path.exists(log_file):
            print(f"✓ 容器日志文件已生成: {log_file}")
            
            # 读取并显示日志内容
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                print(f"✓ 记录了 {len(lines)} 个事件")
                
                for i, line in enumerate(lines[-5:], 1):  # 显示最后5行
                    try:
                        event = json.loads(line.strip())
                        print(f"  事件{i}: {event['event']} - {event['timestamp']}")
                    except:
                        print(f"  事件{i}: {line.strip()}")
        else:
            print("✗ 容器日志文件未生成")
            
    except ImportError as e:
        print(f"✗ 导入容器性能记录模块失败: {e}")
    except Exception as e:
        print(f"✗ 容器测试失败: {e}")

def show_log_files():
    """显示所有日志文件"""
    print("\n=== 日志文件状态 ===")
    
    log_files = [
        '/tmp/faasflow_test_perf.log',
        '/tmp/faasflow_container_perf.log',
        '/tmp/faasflow_perf.log'
    ]
    
    for log_file in log_files:
        if os.path.exists(log_file):
            size = os.path.getsize(log_file)
            print(f"✓ {log_file} - {size} 字节")
        else:
            print(f"✗ {log_file} - 不存在")

if __name__ == '__main__':
    test_performance_logger()
    test_container_performance_logger()
    show_log_files()
    
    print("\n=== 测试完成 ===")
    print("现在可以运行实际的工作流来查看性能记录了！")
