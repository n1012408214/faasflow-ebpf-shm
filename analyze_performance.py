#!/usr/bin/env python3
"""
分析性能日志的脚本
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime

def analyze_log_file(log_file_path):
    """分析日志文件"""
    if not os.path.exists(log_file_path):
        print(f"日志文件不存在: {log_file_path}")
        return
    
    print(f"=== 分析日志文件: {log_file_path} ===")
    
    events = []
    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                events.append(event)
            except json.JSONDecodeError:
                continue
    
    if not events:
        print("没有找到有效的事件")
        return
    
    print(f"总事件数: {len(events)}")
    
    # 按请求ID分组
    requests = defaultdict(list)
    for event in events:
        request_id = event.get('request_id', 'unknown')
        requests[request_id].append(event)
    
    print(f"总请求数: {len(requests)}")
    print()
    
    # 分析每个请求
    for request_id, request_events in requests.items():
        print(f"请求: {request_id}")
        
        # 按事件类型统计
        event_types = defaultdict(int)
        for event in request_events:
            event_type = event.get('event', 'unknown')
            event_types[event_type] += 1
        
        print(f"  事件类型统计:")
        for event_type, count in event_types.items():
            print(f"    {event_type}: {count}")
        
        # 计算总耗时
        start_time = None
        end_time = None
        total_duration = 0
        
        for event in request_events:
            if event.get('event') == 'REQUEST_START':
                start_time = event.get('timestamp')
            elif event.get('event') == 'REQUEST_END':
                end_time = event.get('timestamp')
                total_duration = event.get('duration', 0)
        
        if start_time and end_time:
            print(f"  开始时间: {start_time}")
            print(f"  结束时间: {end_time}")
            print(f"  总耗时: {total_duration:.3f} 秒")
        
        # 分析块执行时间
        block_executions = []
        for event in request_events:
            if event.get('event') == 'BLOCK_EXECUTION_END':
                block_name = event.get('block_name')
                duration = event.get('duration')
                if block_name and duration:
                    block_executions.append((block_name, duration))
        
        if block_executions:
            print(f"  块执行时间:")
            for block_name, duration in block_executions:
                print(f"    {block_name}: {duration:.3f} 秒")
        
        # 分析模型操作
        model_operations = []
        for event in request_events:
            if event.get('event') in ['MODEL_LOAD_END', 'INFERENCE_END']:
                operation = event.get('event')
                duration = event.get('duration')
                if duration:
                    model_operations.append((operation, duration))
        
        if model_operations:
            print(f"  模型操作:")
            for operation, duration in model_operations:
                print(f"    {operation}: {duration:.3f} 秒")
        
        # 分析数据传输时间
        data_transfers = []
        for event in request_events:
            if event.get('event') in ['DATA_FETCH_END', 'DATA_UPLOAD_END', 'DATA_FETCH_LIST_END', 'DATA_FETCH_ITEM_END']:
                event_type = event.get('event')
                block_name = event.get('block_name', 'unknown')
                duration = event.get('duration')
                data_size = event.get('data_size', 0)
                if duration:
                    data_transfers.append((event_type, block_name, duration, data_size))
        
        if data_transfers:
            print(f"  数据传输时间:")
            # 按类型分组统计
            transfer_stats = defaultdict(list)
            for event_type, block_name, duration, data_size in data_transfers:
                transfer_stats[event_type].append((duration, data_size))
            
            for event_type, transfers in transfer_stats.items():
                total_duration = sum(d for d, _ in transfers if d is not None)
                total_size = sum(s for _, s in transfers if s is not None)
                avg_duration = total_duration / len(transfers)
                avg_size = total_size / len(transfers) if total_size > 0 else 0
                print(f"    {event_type}: {len(transfers)}次, 总耗时{total_duration:.3f}s, 平均{avg_duration:.3f}s, 总大小{total_size}bytes, 平均{avg_size:.0f}bytes")
        
        print()

def show_summary(log_file_path):
    """显示性能摘要"""
    if not os.path.exists(log_file_path):
        return
    
    print(f"=== 性能摘要: {log_file_path} ===")
    
    events = []
    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                events.append(event)
            except json.JSONDecodeError:
                continue
    
    if not events:
        return
    
    # 统计事件类型
    event_types = defaultdict(int)
    request_ids = set()
    workflows = set()
    
    for event in events:
        event_type = event.get('event', 'unknown')
        event_types[event_type] += 1
        
        request_id = event.get('request_id')
        if request_id:
            request_ids.add(request_id)
        
        workflow_name = event.get('workflow_name')
        if workflow_name:
            workflows.add(workflow_name)
    
    print(f"总事件数: {len(events)}")
    print(f"总请求数: {len(request_ids)}")
    print(f"工作流类型: {', '.join(workflows)}")
    print()
    
    print("事件类型统计:")
    for event_type, count in sorted(event_types.items()):
        print(f"  {event_type}: {count}")
    
    print()

def main():
    """主函数"""
    log_files = [
        '/home/njl/FaaSFlow/log/container_perf.log'
    ]
    
    print("=== FaaSFlow 性能日志分析 ===")
    print()
    
    # 显示摘要
    for log_file in log_files:
        show_summary(log_file)
    
    # 详细分析
    for log_file in log_files:
        if os.path.exists(log_file):
            analyze_log_file(log_file)
    
    print("=== 分析完成 ===")

if __name__ == '__main__':
    main()
