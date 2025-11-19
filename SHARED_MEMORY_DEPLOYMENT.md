# FaaSFlow 共享内存传输部署指南

## 概述

本文档说明如何在FaaSFlow中启用共享内存传输功能，包括宿主机和容器的配置要求。

## 1. 配置文件修改

### 1.1 启用共享内存传输

在 `config/config.py` 中设置：

```python
# 共享内存配置
ENABLE_SHARED_MEMORY = True  # 启用共享内存传输
SHM_SIZE = 1024 * 1024 * 1024  # 共享内存大小 (1GB)
SHM_FILE_PATH = '/tmp/faasflow_shm'  # 共享内存文件路径
```

### 1.2 共享内存文件准备

在宿主机上创建共享内存文件：

```bash
# 创建共享内存文件
sudo touch /tmp/faasflow_shm
sudo chmod 666 /tmp/faasflow_shm
sudo chown $USER:$USER /tmp/faasflow_shm

# 设置文件大小（可选，程序会自动设置）
sudo truncate -s 1G /tmp/faasflow_shm
```

## 2. 容器启动参数修改

### 2.1 自动修改（推荐）

代码已经自动处理了容器启动参数的修改：

- **共享内存文件挂载**: `/tmp/faasflow_shm` -> 容器内的 `/tmp/faasflow_shm`
- **Unix socket目录挂载**: 宿主机的 `/tmp` -> 容器内的 `/tmp`
- **权限**: 添加了 `NET_ADMIN` 权限

### 2.2 手动修改（如果需要）

如果手动启动容器，需要添加以下参数：

```bash
docker run \
  --mount type=bind,source=/tmp/faasflow_shm,target=/tmp/faasflow_shm \
  --mount type=bind,source=/tmp,target=/tmp \
  --cap-add=NET_ADMIN \
  --label=workflow \
  -p 5000:5000 \
  your_image_name
```

## 3. 系统要求

### 3.1 宿主机要求

- **Linux内核**: 支持共享内存和Unix Domain Socket
- **文件系统**: 支持大文件（>1GB）
- **权限**: 能够创建和访问 `/tmp` 目录下的文件
- **内存**: 至少2GB可用内存（1GB用于共享内存）

### 3.2 容器要求

- **基础镜像**: 支持mmap和socket操作
- **权限**: 需要文件读写权限
- **网络**: 保持原有的网络功能

## 4. 验证部署

### 4.1 检查共享内存文件

```bash
# 检查文件是否存在
ls -la /tmp/faasflow_shm

# 检查文件大小
du -h /tmp/faasflow_shm
```

### 4.2 检查容器挂载

```bash
# 查看容器挂载
docker inspect <container_id> | grep -A 10 "Mounts"

# 在容器内检查挂载
docker exec <container_id> ls -la /tmp/faasflow_shm
```

### 4.3 检查Unix socket

```bash
# 检查socket文件
ls -la /tmp/faasflow_container_*.sock
ls -la /proxy/mnt/container_*.sock
```

## 5. 故障排除

### 5.1 共享内存访问失败

**症状**: 容器无法访问共享内存文件

**解决方案**:
```bash
# 检查文件权限
sudo chmod 666 /tmp/faasflow_shm
sudo chown $USER:$USER /tmp/faasflow_shm

# 检查挂载
docker exec <container_id> ls -la /tmp/faasflow_shm
```

### 5.2 Unix socket连接失败

**症状**: 容器间通信失败

**解决方案**:
```bash
# 检查socket文件权限
sudo chmod 777 /tmp
sudo chmod 666 /tmp/faasflow_container_*.sock

# 检查网络命名空间
docker exec <container_id> ip addr show
```

### 5.3 内存不足

**症状**: 共享内存分配失败

**解决方案**:
```bash
# 检查可用内存
free -h

# 减少共享内存大小
# 在 config.py 中修改 SHM_SIZE
SHM_SIZE = 512 * 1024 * 1024  # 512MB
```

## 6. 性能优化

### 6.1 共享内存大小调优

根据工作负载调整共享内存大小：

```python
# 小工作负载
SHM_SIZE = 256 * 1024 * 1024  # 256MB

# 中等工作负载
SHM_SIZE = 1024 * 1024 * 1024  # 1GB

# 大工作负载
SHM_SIZE = 2048 * 1024 * 1024  # 2GB
```

### 6.2 文件系统优化

使用tmpfs提高性能：

```bash
# 将共享内存放在tmpfs中
sudo mount -t tmpfs -o size=1G tmpfs /tmp/faasflow_shm_tmpfs
sudo ln -s /tmp/faasflow_shm_tmpfs/faasflow_shm /tmp/faasflow_shm
```

## 7. 回退方案

如果共享内存传输出现问题，可以快速回退到网络传输：

```python
# 在 config.py 中设置
ENABLE_SHARED_MEMORY = False
```

系统会自动使用原有的网络传输方案，无需重启容器。

## 8. 监控和日志

### 8.1 共享内存使用情况

```bash
# 监控共享内存使用
watch -n 1 'ls -la /tmp/faasflow_shm'

# 查看系统日志
journalctl -f | grep faasflow
```

### 8.2 性能指标

- **传输延迟**: 共享内存 vs 网络传输
- **吞吐量**: 数据传输速率
- **内存使用**: 共享内存占用情况
- **错误率**: 传输失败次数

## 9. 安全考虑

### 9.1 文件权限

确保共享内存文件权限适当：

```bash
# 设置适当的权限
sudo chmod 600 /tmp/faasflow_shm
sudo chown faasflow:faasflow /tmp/faasflow_shm
```

### 9.2 网络隔离

保持容器网络隔离：

```bash
# 使用自定义网络
docker network create faasflow_network
docker run --network faasflow_network ...
```

## 10. 总结

共享内存传输功能通过以下方式实现：

1. **宿主机**: 创建共享内存文件和Unix socket
2. **容器**: 挂载共享内存文件和socket目录
3. **应用**: 使用共享内存进行数据传输

这种方案在保持向后兼容的同时，显著提升了数据传输性能。
