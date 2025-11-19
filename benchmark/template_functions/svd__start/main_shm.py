#!/usr/bin/env python3
"""
支持共享内存的SVD函数主程序
"""

import time
import numpy as np
import os
import sys
import json
import pickle

# 添加共享内存模块路径
sys.path.append('/proxy')

try:
    from shm_utils import FaaSFlowShmManager, is_shm_enabled
    SHM_AVAILABLE = True
    print("共享内存模块导入成功")
except ImportError as e:
    SHM_AVAILABLE = False
    print(f"警告: 共享内存模块不可用，将使用HTTP模式: {e}")

def run_with_shm():
    """使用共享内存模式运行"""
    print("使用共享内存模式运行SVD函数")
    
    # 初始化共享内存管理器
    shm_manager = FaaSFlowShmManager()
    if not shm_manager.init_shm_pool():
        print("共享内存池初始化失败，回退到HTTP模式")
        return run_without_shm()
    
    try:
        # 生成测试数据
        st = time.time()
        print("生成随机矩阵数据...")
        m = np.random.randint(0, 100, (8192, 512))
        
        # 执行SVD分解
        print("执行SVD分解...")
        Sigma = np.linalg.svd(m, compute_uv=False)
        ed = time.time()
        duration = ed - st
        
        print(f"SVD分解完成，耗时: {duration:.4f}秒")
        
        # 分割结果
        res = np.split(m, 16, axis=0)
        
        # 通过共享内存输出结果
        print("通过共享内存输出结果...")
        for i, data in enumerate(res):
            # 序列化数据
            data_bytes = pickle.dumps(data)
            
            # 分配共享内存包
            packet_id = shm_manager.allocate_packet(len(data_bytes), function_id=1)
            if packet_id is None:
                print(f"无法分配共享内存包 {i}")
                continue
            
            # 写入数据到共享内存
            shm_manager.write_data_to_packet(packet_id, data_bytes)
            
            # 创建包描述符
            descriptor = {
                'packet_id': packet_id,
                'data_size': len(data_bytes),
                'function_id': 1,
                'timestamp': int(time.time() * 1000000)
            }
            
            # 通过store输出描述符
            store.post('matrix_shm', json.dumps(descriptor), datatype='json', serial_num=i)
            print(f"输出矩阵块 {i}: 大小={len(data_bytes)} bytes, 包ID={packet_id}")
        
        # 输出时间信息
        time_info = {
            'st': st,
            'ed': ed,
            'duration': duration,
            'mode': 'shared_memory'
        }
        store.post('save_db', json.dumps(time_info), datatype='json', serial_num=0)
        
        print(f"SVD函数执行完成，共输出16个矩阵块")
        
    except Exception as e:
        print(f"共享内存模式执行失败: {e}")
        return run_without_shm()
    finally:
        # 清理共享内存
        shm_manager.cleanup()

def run_without_shm():
    """使用HTTP模式运行（原始方式）"""
    print("使用HTTP模式运行SVD函数")
    
    st = time.time()
    m = np.random.randint(0, 100, (8192, 512))
    Sigma = np.linalg.svd(m, compute_uv=False)
    ed = time.time()
    
    res = np.split(m, 16, axis=0)
    for i, data in enumerate(res):
        store.post('matrix', data.tobytes(), datatype='octet', serial_num=i)
    
    store.post('save_db', {'st': st, 'ed': ed, 'duration': ed - st, 'mode': 'http'}, datatype='json', serial_num=0)
    print(f"SVD函数执行完成，使用HTTP模式")

if __name__ == "__main__":
    # 检查是否启用共享内存
    if SHM_AVAILABLE and is_shm_enabled():
        run_with_shm()
    else:
        run_without_shm()



