#!/bin/bash

echo "=== FaaSFlow 容器测试脚本 ==="

# 检查Docker是否安装
if ! command -v docker &> /dev/null; then
    echo "错误: Docker未安装"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "错误: docker-compose未安装"
    exit 1
fi

# 清理旧的容器和镜像
echo "清理旧的容器和镜像..."
docker-compose down
docker system prune -f

# 构建镜像
echo "构建Docker镜像..."
docker-compose build

# 启动Gateway（在宿主机上）
echo "启动Gateway（宿主机）..."
./gateway &
GATEWAY_PID=$!

# 等待Gateway启动
sleep 3

# 启动容器服务
echo "启动容器服务..."
docker-compose up -d

# 等待服务启动
echo "等待服务启动..."
sleep 5

# 检查容器状态
echo "检查容器状态..."
docker-compose ps

# 测试函数调用
echo ""
echo "=== 开始测试 ==="

# 测试函数1
echo "测试函数1..."
curl -X POST http://localhost:8080/function/1 -d "Hello from Container Function 1"
echo ""

# 测试函数2
echo "测试函数2..."
curl -X POST http://localhost:8080/function/2 -d "Hello from Container Function 2"
echo ""

# 测试函数3
echo "测试函数3..."
curl -X POST http://localhost:8080/function/3 -d "Hello from Container Function 3"
echo ""

# 并发测试
echo "并发测试..."
for i in {1..5}; do
    curl -X POST http://localhost:8080/function/1 -d "Concurrent Request $i" &
done
wait
echo ""

# 查看容器日志
echo "查看容器日志..."
echo "Proxy日志:"
docker logs faasflow-proxy --tail 10

echo ""
echo "Func1日志:"
docker logs faasflow-func1 --tail 10

echo ""
echo "Func2日志:"
docker logs faasflow-func2 --tail 10

echo ""
echo "Func3日志:"
docker logs faasflow-func3 --tail 10

# 清理
echo ""
echo "=== 清理资源 ==="
echo "停止Gateway..."
kill $GATEWAY_PID 2>/dev/null

echo "停止容器..."
docker-compose down

echo "测试完成！"






