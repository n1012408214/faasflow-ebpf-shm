# 共享内存监控指南

本文档介绍如何使用外部监控工具和命令来监控 FaaSFlow 共享内存的使用情况。

## 共享内存配置

根据配置，项目使用以下共享内存：
- **名称**: `faasflow_shm_opt` (优化版本，5GB)
- **备选**: `faasflow_shm` (旧版本，100MB)
- **类型**: POSIX 共享内存 (通过 `multiprocessing.shared_memory`)

## 监控方法

### 1. 查看 POSIX 共享内存文件 (`/dev/shm/`)

POSIX 共享内存会被映射到 `/dev/shm/` 目录下：

```bash
# 查看所有共享内存文件
ls -lah /dev/shm/

# 查看特定共享内存文件
ls -lah /dev/shm/faasflow_shm_opt

# 查看共享内存文件大小
du -h /dev/shm/faasflow_shm_opt

# 查看共享内存文件详细信息
stat /dev/shm/faasflow_shm_opt

# 实时监控共享内存大小变化
watch -n 1 'ls -lh /dev/shm/faasflow_shm_opt'
```

### 2. 查看文件系统使用情况 (`df`)

```bash
# 查看 /dev/shm 文件系统使用情况
df -h /dev/shm

# 实时监控
watch -n 1 'df -h /dev/shm'
```

### 3. 查看哪些进程在使用共享内存 (`lsof`)

```bash
# 查看打开特定共享内存文件的进程
lsof /dev/shm/faasflow_shm_opt

# 查看所有共享内存文件的进程
lsof /dev/shm/*

# 查看特定进程打开的共享内存
lsof -p <PID> | grep /dev/shm
```

### 4. 查看进程内存映射 (`pmap`)

```bash
# 查看进程的内存映射（包括共享内存）
pmap -x <PID>

# 查看进程的共享内存映射
pmap -x <PID> | grep -i shm

# 查看进程的内存使用摘要
pmap -d <PID>
```

### 5. 查看系统V IPC 共享内存 (`ipcs`)

虽然项目使用 POSIX 共享内存，但也可以查看系统V IPC：

```bash
# 查看所有 IPC 资源（共享内存、信号量、消息队列）
ipcs -a

# 只查看共享内存
ipcs -m

# 查看共享内存详细信息
ipcs -m -i <shmid>

# 以人类可读格式显示
ipcs -m -h

# 显示创建者信息
ipcs -m -c
```

### 6. 查看 `/proc` 文件系统

```bash
# 查看系统V IPC 共享内存
cat /proc/sysvipc/shm

# 查看特定进程的内存映射
cat /proc/<PID>/maps | grep shm

# 查看进程的内存使用情况
cat /proc/<PID>/status | grep -i shm

# 查看进程的内存映射详细信息
cat /proc/<PID>/smaps | grep -A 20 shm
```

### 7. 使用系统监控工具

#### `htop` / `top`

```bash
# 安装 htop
sudo apt-get install htop

# 运行 htop，按 F5 查看树状结构，可以看到进程的内存使用
htop

# 或者使用 top
top
```

#### `free` - 查看系统内存

```bash
# 查看系统内存使用情况（包括共享内存）
free -h

# 实时监控
watch -n 1 'free -h'
```

#### `smem` - 共享内存统计工具

```bash
# 安装 smem
sudo apt-get install smem

# 查看共享内存使用情况
smem

# 按进程分组
smem -P faasflow

# 显示百分比
smem -P faasflow -s pss
```

### 8. 使用 `fuser` 查看使用共享内存的进程

```bash
# 查看哪些进程在使用共享内存文件
fuser /dev/shm/faasflow_shm_opt

# 显示详细信息
fuser -v /dev/shm/faasflow_shm_opt

# 显示进程名称
fuser -v -m /dev/shm/faasflow_shm_opt
```

### 9. 使用 `strace` 跟踪共享内存操作

```bash
# 跟踪进程的共享内存操作
strace -p <PID> -e trace=shm_open,shm_unlink,mmap,munmap

# 或者跟踪特定系统调用
strace -p <PID> -e trace=file | grep shm
```

### 10. 编写监控脚本

创建一个简单的监控脚本：

```bash
#!/bin/bash
# monitor_shm.sh

SHM_NAME="faasflow_shm_opt"
SHM_PATH="/dev/shm/$SHM_NAME"

while true; do
    clear
    echo "=== 共享内存监控 ==="
    echo "时间: $(date)"
    echo ""
    
    if [ -f "$SHM_PATH" ]; then
        echo "共享内存文件: $SHM_PATH"
        echo "大小: $(du -h $SHM_PATH | cut -f1)"
        echo "权限: $(stat -c '%a %U:%G' $SHM_PATH)"
        echo ""
        echo "使用该共享内存的进程:"
        lsof $SHM_PATH 2>/dev/null | tail -n +2
        echo ""
        echo "文件系统使用情况:"
        df -h /dev/shm
    else
        echo "共享内存文件不存在: $SHM_PATH"
    fi
    
    sleep 2
done
```

使用方式：
```bash
chmod +x monitor_shm.sh
./monitor_shm.sh
```

### 11. 使用 `inotifywait` 监控共享内存文件变化

```bash
# 安装 inotify-tools
sudo apt-get install inotify-tools

# 监控共享内存文件的变化
inotifywait -m /dev/shm/faasflow_shm_opt
```

### 12. 使用 `systemd-cgtop` 监控容器资源（如果使用 systemd）

```bash
# 查看容器资源使用情况
systemd-cgtop

# 查看特定服务的资源使用
systemctl status <service-name>
```

### 13. Docker 容器监控（如果共享内存在容器中）

```bash
# 查看容器资源使用情况
docker stats

# 查看特定容器的资源使用
docker stats <container-id>

# 查看容器的详细信息
docker inspect <container-id> | grep -i shm

# 进入容器查看
docker exec -it <container-id> ls -lah /dev/shm/
```

## 实用的监控命令组合

### 快速查看共享内存状态

```bash
# 一键查看共享内存状态
echo "=== 共享内存文件 ===" && \
ls -lh /dev/shm/faasflow_shm_opt 2>/dev/null || echo "共享内存不存在" && \
echo "" && \
echo "=== 文件系统使用 ===" && \
df -h /dev/shm && \
echo "" && \
echo "=== 使用进程 ===" && \
lsof /dev/shm/faasflow_shm_opt 2>/dev/null || echo "没有进程使用"
```

### 持续监控脚本

```bash
#!/bin/bash
# continuous_monitor.sh

SHM_NAME="faasflow_shm_opt"
INTERVAL=2

while true; do
    clear
    echo "=========================================="
    echo "共享内存监控 - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=========================================="
    echo ""
    
    # 文件大小
    if [ -f "/dev/shm/$SHM_NAME" ]; then
        SIZE=$(du -h /dev/shm/$SHM_NAME | cut -f1)
        echo "文件大小: $SIZE"
    else
        echo "状态: 共享内存不存在"
    fi
    
    # 文件系统使用
    echo ""
    echo "文件系统使用:"
    df -h /dev/shm | tail -n +2
    
    # 使用进程数
    PROC_COUNT=$(lsof /dev/shm/$SHM_NAME 2>/dev/null | wc -l)
    echo ""
    echo "使用进程数: $((PROC_COUNT - 1))"  # 减去标题行
    
    # 系统内存
    echo ""
    echo "系统内存:"
    free -h | grep -E "Mem|Swap"
    
    sleep $INTERVAL
done
```

## 监控指标说明

### 关键指标

1. **文件大小**: `/dev/shm/faasflow_shm_opt` 的大小（应该是 5GB）
2. **文件系统使用率**: `/dev/shm` 的使用率（不应该超过 80%）
3. **使用进程数**: 有多少进程正在使用该共享内存
4. **进程内存映射**: 每个进程映射的共享内存大小

### 警告阈值

- **文件系统使用率 > 80%**: 需要清理或扩容
- **使用进程数异常**: 可能存在内存泄漏
- **文件大小异常**: 可能配置错误

## 故障排查

### 共享内存不存在

```bash
# 检查共享内存是否被创建
ls -lah /dev/shm/faasflow_shm_opt

# 检查进程是否在运行
ps aux | grep faasflow

# 检查日志
journalctl -u <service-name> -f
```

### 共享内存无法访问

```bash
# 检查权限
ls -l /dev/shm/faasflow_shm_opt

# 检查 SELinux（如果启用）
getenforce

# 检查文件系统挂载
mount | grep shm
```

### 共享内存泄漏

```bash
# 查看所有使用共享内存的进程
lsof /dev/shm/faasflow_shm_opt

# 检查进程内存使用
ps aux --sort=-%mem | head -20

# 查看进程内存映射
pmap -x <PID>
```

## 参考资料

- [Linux 共享内存文档](https://man7.org/linux/man-pages/man7/shm_overview.7.html)
- [POSIX 共享内存](https://man7.org/linux/man-pages/man3/shm_open.3.html)
- [multiprocessing.shared_memory 文档](https://docs.python.org/3/library/multiprocessing.shared_memory.html)

