# FaaSFlow 部署指南

## 服务器配置

根据您的需求，FaaSFlow框架已配置为以下服务器架构：

- **Gateway节点**: 192.168.2.156
- **Storage节点**: 192.168.2.157  
- **Worker1节点**: 192.168.2.154
- **Worker2节点**: 192.168.2.155

## 部署步骤

### 1. 在所有节点上克隆代码

```bash
git clone https://github.com/lzjzx1122/FaaSFlow.git
cd FaaSFlow
```

### 2. Storage节点 (192.168.2.157) 设置

在Storage节点上运行以下命令：

```bash
# 安装依赖和启动Kafka、CouchDB
bash scripts/db_setup.bash
```

这将安装：
- Docker
- Kafka (用于数据传输)
- CouchDB (用于日志收集)
- Python依赖包

### 3. Gateway节点 (192.168.2.156) 设置

在Gateway节点上运行：

```bash
# 安装依赖
bash scripts/gateway_setup.bash
```

### 4. Worker节点设置

在每个Worker节点 (192.168.2.154, 192.168.2.155) 上运行：

```bash
# 安装依赖和构建Docker镜像
bash scripts/worker_setup.bash
```

这将安装：
- Docker
- Redis
- Python依赖包
- 构建4个基准测试的Docker镜像

## 启动服务

### 1. 启动Gateway

在Gateway节点 (192.168.2.156) 上：

```bash
cd src/workflow_manager
python3 gateway.py 192.168.2.156 7000
```

### 2. 启动Worker代理

在每个Worker节点上：

**Worker1 (192.168.2.154):**
```bash
cd src/workflow_manager
python3 test_server.py 192.168.2.154
```

**Worker2 (192.168.2.155):**
```bash
cd src/workflow_manager
python3 test_server.py 192.168.2.155
```

## 运行测试

在Gateway节点上运行测试脚本：

### 响应延迟测试
```bash
cd test
python3 async_test.py
```

### 峰值吞吐量测试
```bash
cd test
python3 sync_test.py
```

### 多工作流共存测试
```bash
cd test
python3 async_colocation_test.py
```

## 注意事项

1. **重启建议**: 每次运行测试脚本前，建议重启所有`test_server.py`和`gateway.py`进程，以避免潜在问题并清理内存空间。

2. **硬件要求**:
   - Gateway和Storage节点: 最少8核CPU，16GB内存，200GB SSD
   - Worker节点: 最少16核CPU，64GB内存，200GB SSD
   - 所有节点运行Ubuntu 20.04

3. **网络要求**: 确保所有节点之间网络连通，特别是：
   - Gateway节点可以访问所有Worker节点
   - 所有节点可以访问Storage节点
   - 端口7000 (Gateway), 9092 (Kafka), 5984 (CouchDB), 6379 (Redis) 需要开放

## 故障排除

如果遇到连接问题，请检查：
1. 防火墙设置
2. 网络连通性
3. 服务端口是否正常监听
4. Docker容器状态

## 配置验证

可以通过以下方式验证配置：

1. 检查Gateway日志，确认能连接到Worker节点
2. 检查Worker日志，确认能连接到Storage节点
3. 运行简单的测试脚本验证端到端功能


