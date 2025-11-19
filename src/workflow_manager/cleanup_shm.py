#!/usr/bin/env python3
"""
清理共享内存脚本
"""

import multiprocessing.shared_memory as shm
import os

def cleanup_shared_memory():
    """清理共享内存"""
    try:
        shm_obj = shm.SharedMemory(name='faasflow_shm')
        shm_obj.close()
        shm_obj.unlink()
        print("✓ 共享内存清理成功")
    except FileNotFoundError:
        print("✓ 共享内存不存在，无需清理")
    except Exception as e:
        print(f"✗ 清理失败: {e}")

if __name__ == "__main__":
    cleanup_shared_memory()
