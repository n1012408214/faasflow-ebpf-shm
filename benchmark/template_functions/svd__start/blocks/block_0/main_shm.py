#!/usr/bin/env python3
"""
支持共享内存的SVD Block函数
"""

import time
import numpy as np
import sys
import json
import pickle

# 添加共享内存模块路径
sys.path.append('/proxy')

try:
    from shm_utils import FaaSFlowShmManager, is_shm_enabled
    SHM_AVAILABLE = True
    print("Block: 共享内存模块导入成功")
except ImportError as e:
    SHM_AVAILABLE = False
    print(f"Block: 警告: 共享内存模块不可用，将使用HTTP模式: {e}")

def run_with_shm():
    """使用共享内存模式运行Block"""
    print("Block: 使用共享内存模式运行")
    
    # 初始化共享内存管理器
    shm_manager = FaaSFlowShmManager()
    if not shm_manager.init_shm_pool():
        print("Block: 共享内存池初始化失败，回退到HTTP模式")
        return run_without_shm()
    
    try:
        # 加载预生成的矩阵数据
        print("Block: 加载矩阵数据...")
        m = np.load('/proxy/data.npy')
        res = np.split(m, 16, axis=0)
        
        print(f"Block: 矩阵数据加载完成，大小: {m.shape}, 分割为16块")
        
        # 通过共享内存输出每块数据
        print("Block: 通过共享内存输出矩阵块...")
        for i, data in enumerate(res):
            # 序列化数据
            data_bytes = pickle.dumps(data)
            
            # 分配共享内存包
            packet_id = shm_manager.allocate_packet(len(data_bytes), function_id=1)
            if packet_id is None:
                print(f"Block: 无法分配共享内存包 {i}")
                continue
            
            # 写入数据到共享内存
            shm_manager.write_data_to_packet(packet_id, data_bytes)
            
            # 创建包描述符
            descriptor = {
                'packet_id': packet_id,
                'data_size': len(data_bytes),
                'function_id': 1,
                'block_id': i,
                'timestamp': int(time.time() * 1000000)
            }
            
            # 通过store输出描述符
            store.post('matrix_block_shm', json.dumps(descriptor), datatype='json', serial_num=i)
            print(f"Block: 输出矩阵块 {i}: 大小={len(data_bytes)} bytes, 包ID={packet_id}")
        
        print(f"Block: 执行完成，共输出16个矩阵块")
        
    except Exception as e:
        print(f"Block: 共享内存模式执行失败: {e}")
        return run_without_shm()
    finally:
        # 清理共享内存
        shm_manager.cleanup()

def run_without_shm():
    """使用HTTP模式运行Block（原始方式）"""
    print("Block: 使用HTTP模式运行")
    
    m = np.load('/proxy/data.npy')
    res = np.split(m, 16, axis=0)
    
    for data in res:
        store.post('matrix', data.dumps(), datatype='octet')
    
    print(f"Block: 执行完成，使用HTTP模式")

if __name__ == "__main__":
    # 检查是否启用共享内存
    if SHM_AVAILABLE and is_shm_enabled():
        run_with_shm()
    else:
        run_without_shm()



