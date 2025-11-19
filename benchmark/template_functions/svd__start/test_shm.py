#!/usr/bin/env python3
"""
测试SVD函数的共享内存功能
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
    print("测试: 共享内存模块导入成功")
except ImportError as e:
    SHM_AVAILABLE = False
    print(f"测试: 警告: 共享内存模块不可用: {e}")

def test_shm_svd():
    """测试共享内存SVD功能"""
    if not SHM_AVAILABLE or not is_shm_enabled():
        print("测试: 共享内存不可用，跳过测试")
        return False
    
    print("测试: 开始测试共享内存SVD功能")
    
    # 初始化共享内存管理器
    shm_manager = FaaSFlowShmManager()
    if not shm_manager.init_shm_pool():
        print("测试: 共享内存池初始化失败")
        return False
    
    try:
        # 生成测试数据
        print("测试: 生成测试矩阵...")
        m = np.random.randint(0, 100, (1024, 256))  # 较小的测试矩阵
        original_shape = m.shape
        
        # 执行SVD
        print("测试: 执行SVD分解...")
        st = time.time()
        Sigma = np.linalg.svd(m, compute_uv=False)
        ed = time.time()
        svd_time = ed - st
        
        print(f"测试: SVD完成，耗时: {svd_time:.4f}秒")
        
        # 分割结果
        res = np.split(m, 4, axis=0)  # 分割为4块
        
        # 测试共享内存写入
        print("测试: 测试共享内存写入...")
        packet_ids = []
        for i, data in enumerate(res):
            data_bytes = pickle.dumps(data)
            packet_id = shm_manager.allocate_packet(len(data_bytes), function_id=1)
            if packet_id is None:
                print(f"测试: 无法分配包 {i}")
                continue
            
            shm_manager.write_data_to_packet(packet_id, data_bytes)
            packet_ids.append(packet_id)
            print(f"测试: 写入包 {i}: ID={packet_id}, 大小={len(data_bytes)} bytes")
        
        # 测试共享内存读取
        print("测试: 测试共享内存读取...")
        for i, packet_id in enumerate(packet_ids):
            data_bytes = shm_manager.read_data_from_packet(packet_id)
            if data_bytes is None:
                print(f"测试: 无法读取包 {i}")
                continue
            
            data = pickle.loads(data_bytes)
            print(f"测试: 读取包 {i}: ID={packet_id}, 数据形状={data.shape}")
            
            # 验证数据完整性
            if data.shape == res[i].shape and np.array_equal(data, res[i]):
                print(f"测试: 包 {i} 数据验证通过")
            else:
                print(f"测试: 包 {i} 数据验证失败")
                return False
        
        # 清理共享内存
        for packet_id in packet_ids:
            shm_manager.free_packet(packet_id)
        
        print("测试: 共享内存SVD功能测试通过")
        return True
        
    except Exception as e:
        print(f"测试: 测试失败: {e}")
        return False
    finally:
        shm_manager.cleanup()

def test_performance_comparison():
    """性能对比测试"""
    print("测试: 开始性能对比测试")
    
    # 生成测试数据
    sizes = [(512, 256), (1024, 512), (2048, 1024)]
    
    for rows, cols in sizes:
        print(f"\n测试: 矩阵大小 {rows}x{cols}")
        
        # 生成矩阵
        m = np.random.randint(0, 100, (rows, cols))
        data_size = m.nbytes
        
        # 测试SVD性能
        st = time.time()
        Sigma = np.linalg.svd(m, compute_uv=False)
        ed = time.time()
        svd_time = ed - st
        
        print(f"测试: 数据大小: {data_size/1024/1024:.2f} MB")
        print(f"测试: SVD耗时: {svd_time:.4f}秒")
        print(f"测试: 处理速度: {data_size/1024/1024/svd_time:.2f} MB/s")

if __name__ == "__main__":
    print("=== SVD共享内存功能测试 ===")
    
    # 基础功能测试
    success = test_shm_svd()
    
    if success:
        print("\n=== 性能测试 ===")
        test_performance_comparison()
    
    print("\n=== 测试完成 ===")



