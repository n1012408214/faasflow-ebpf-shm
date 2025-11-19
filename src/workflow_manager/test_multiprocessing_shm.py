#!/usr/bin/env python3
"""
Multiprocessing共享内存管理器测试脚本
"""

import sys
import os
import time
import threading
import logging

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from multiprocessing_shm import (
    MultiprocessingSharedMemoryManager, 
    create_shm_packet, 
    read_shm_packet,
    create_shm_packet_global,
    read_shm_packet_global
)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_basic_functionality():
    """测试基本功能"""
    print("=== 基本功能测试 ===")
    
    try:
        with MultiprocessingSharedMemoryManager() as shm:
            print("✓ 共享内存管理器初始化成功")
            
            # 测试包分配
            test_data = b"Hello, Multiprocessing Shared Memory Test!"
            result = shm.allocate_packet(len(test_data), 1)
            
            if result:
                packet_id, data_view = result
                print(f"✓ 包分配成功: ID={packet_id}")
                
                # 写入数据
                data_view[:] = test_data
                print("✓ 数据写入成功")
                
                # 读取数据
                read_data = shm.get_packet_data(packet_id)
                if read_data == test_data:
                    print("✓ 数据读取成功")
                else:
                    print("✗ 数据读取失败")
                    return False
                
                # 释放包
                if shm.free_packet(packet_id):
                    print("✓ 包释放成功")
                else:
                    print("✗ 包释放失败")
                    return False
                
                return True
            else:
                print("✗ 包分配失败")
                return False
                
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        return False

def test_concurrent_access():
    """测试并发访问"""
    print("\n=== 并发访问测试 ===")
    
    def worker(worker_id, shm, results):
        try:
            test_data = f"Worker {worker_id} data".encode()
            result = shm.allocate_packet(len(test_data), worker_id)
            
            if result:
                packet_id, data_view = result
                
                # 写入数据
                data_view[:] = test_data
                
                # 读取数据
                read_data = shm.get_packet_data(packet_id)
                
                # 释放包
                shm.free_packet(packet_id)
                
                results[worker_id] = read_data == test_data
                print(f"✓ Worker {worker_id} 完成")
            else:
                results[worker_id] = False
                print(f"✗ Worker {worker_id} 失败")
                
        except Exception as e:
            results[worker_id] = False
            print(f"✗ Worker {worker_id} 异常: {e}")
    
    try:
        with MultiprocessingSharedMemoryManager() as shm:
            results = {}
            threads = []
            
            # 创建多个工作线程
            for i in range(5):
                results[i] = False
                thread = threading.Thread(target=worker, args=(i, shm, results))
                threads.append(thread)
                thread.start()
            
            # 等待所有线程完成
            for thread in threads:
                thread.join()
            
            success_count = sum(results.values())
            print(f"✓ 并发测试完成: {success_count}/{len(results)} 成功")
            
            return success_count == len(results)
            
    except Exception as e:
        print(f"✗ 并发测试失败: {e}")
        return False

def test_convenience_functions():
    """测试便捷函数"""
    print("\n=== 便捷函数测试 ===")
    
    try:
        with MultiprocessingSharedMemoryManager() as shm:
            test_data = b"Convenience function test data"
            
            # 使用便捷函数创建包
            packet_id = create_shm_packet(test_data, 2, shm)
            if packet_id:
                print(f"✓ 便捷函数创建包成功: ID={packet_id}")
                
                # 使用便捷函数读取包
                read_data = read_shm_packet(packet_id, shm)
                if read_data == test_data:
                    print("✓ 便捷函数读取包成功")
                    
                    # 释放包
                    shm.free_packet(packet_id)
                    return True
                else:
                    print("✗ 便捷函数读取包失败")
                    return False
            else:
                print("✗ 便捷函数创建包失败")
                return False
                
    except Exception as e:
        print(f"✗ 便捷函数测试失败: {e}")
        return False

def test_global_convenience_functions():
    """测试全局便捷函数"""
    print("\n=== 全局便捷函数测试 ===")
    
    try:
        test_data = b"Global convenience function test data"
        
        # 使用全局便捷函数创建包
        packet_id = create_shm_packet_global(test_data, 3)
        if packet_id:
            print(f"✓ 全局便捷函数创建包成功: ID={packet_id}")
            
            # 使用全局便捷函数读取包
            read_data = read_shm_packet_global(packet_id)
            if read_data == test_data:
                print("✓ 全局便捷函数读取包成功")
                
                # 释放包（需要获取全局管理器）
                from multiprocessing_shm import get_global_shm_manager
                shm_manager = get_global_shm_manager()
                shm_manager.free_packet(packet_id)
                return True
            else:
                print("✗ 全局便捷函数读取包失败")
                return False
        else:
            print("✗ 全局便捷函数创建包失败")
            return False
            
    except Exception as e:
        print(f"✗ 全局便捷函数测试失败: {e}")
        return False

def test_error_handling():
    """测试错误处理"""
    print("\n=== 错误处理测试 ===")
    
    try:
        with MultiprocessingSharedMemoryManager() as shm:
            # 测试不存在的包
            result = shm.get_packet_data(99999)
            if result is None:
                print("✓ 不存在的包处理正确")
            else:
                print("✗ 不存在的包处理错误")
                return False
            
            # 测试释放不存在的包
            result = shm.free_packet(99999)
            if not result:
                print("✓ 释放不存在的包处理正确")
            else:
                print("✗ 释放不存在的包处理错误")
                return False
            
            # 测试过大的数据
            large_data = b"x" * (64 * 1024 + 1)  # 超过MAX_PACKET_SIZE
            result = shm.allocate_packet(len(large_data), 1)
            if result is None:
                print("✓ 过大数据处理正确")
            else:
                print("✗ 过大数据处理错误")
                return False
            
            return True
            
    except Exception as e:
        print(f"✗ 错误处理测试失败: {e}")
        return False

def test_performance():
    """测试性能"""
    print("\n=== 性能测试 ===")
    
    try:
        with MultiprocessingSharedMemoryManager() as shm:
            # 测试大量小包
            start_time = time.time()
            packet_ids = []
            
            for i in range(100):
                test_data = f"Packet {i} data".encode()
                result = shm.allocate_packet(len(test_data), i % 5)
                if result:
                    packet_id, data_view = result
                    data_view[:] = test_data
                    packet_ids.append(packet_id)
                else:
                    print(f"✗ 包 {i} 分配失败")
                    return False
            
            # 读取所有包
            for i, packet_id in enumerate(packet_ids):
                read_data = shm.get_packet_data(packet_id)
                expected_data = f"Packet {i} data".encode()
                if read_data != expected_data:
                    print(f"✗ 包 {i} 数据不匹配")
                    return False
            
            # 释放所有包
            for packet_id in packet_ids:
                shm.free_packet(packet_id)
            
            end_time = time.time()
            elapsed = end_time - start_time
            
            print(f"✓ 性能测试完成: 100个包, 耗时 {elapsed:.3f} 秒")
            print(f"  平均每个包: {elapsed/100*1000:.2f} 毫秒")
            
            return True
            
    except Exception as e:
        print(f"✗ 性能测试失败: {e}")
        return False

def test_multiprocess_access():
    """测试多进程访问"""
    print("\n=== 多进程访问测试 ===")
    
    import multiprocessing as mp
    
    def process_worker(worker_id, results):
        try:
            # 在子进程中创建共享内存管理器
            with MultiprocessingSharedMemoryManager() as shm:
                test_data = f"Process {worker_id} data".encode()
                result = shm.allocate_packet(len(test_data), worker_id)
                
                if result:
                    packet_id, data_view = result
                    data_view[:] = test_data
                    
                    # 读取数据
                    read_data = shm.get_packet_data(packet_id)
                    
                    # 释放包
                    shm.free_packet(packet_id)
                    
                    results[worker_id] = read_data == test_data
                    print(f"✓ Process {worker_id} 完成")
                else:
                    results[worker_id] = False
                    print(f"✗ Process {worker_id} 失败")
                    
        except Exception as e:
            results[worker_id] = False
            print(f"✗ Process {worker_id} 异常: {e}")
    
    try:
        # 使用Manager来共享结果
        with mp.Manager() as manager:
            results = manager.dict()
            processes = []
            
            # 创建多个进程
            for i in range(3):
                results[i] = False
                process = mp.Process(target=process_worker, args=(i, results))
                processes.append(process)
                process.start()
            
            # 等待所有进程完成
            for process in processes:
                process.join()
            
            success_count = sum(results.values())
            print(f"✓ 多进程测试完成: {success_count}/{len(results)} 成功")
            
            return success_count == len(results)
            
    except Exception as e:
        print(f"✗ 多进程测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("FaaSFlow Multiprocessing共享内存管理器测试")
    print("=" * 60)
    
    tests = [
        ("基本功能", test_basic_functionality),
        ("并发访问", test_concurrent_access),
        ("便捷函数", test_convenience_functions),
        ("全局便捷函数", test_global_convenience_functions),
        ("错误处理", test_error_handling),
        ("性能测试", test_performance),
        ("多进程访问", test_multiprocess_access)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                print(f"✓ {test_name} 测试通过")
                passed += 1
            else:
                print(f"✗ {test_name} 测试失败")
        except Exception as e:
            print(f"✗ {test_name} 测试异常: {e}")
    
    print("\n" + "=" * 60)
    print(f"测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过！")
        print("\nMultiprocessing共享内存管理器特点:")
        print("✅ 使用Python标准库")
        print("✅ 跨平台兼容")
        print("✅ 支持多进程")
        print("✅ 高性能零拷贝")
        print("✅ 自动资源管理")
        return 0
    else:
        print("❌ 部分测试失败")
        return 1

if __name__ == "__main__":
    sys.exit(main())
