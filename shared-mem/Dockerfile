FROM ubuntu:20.04

# 设置环境变量
ENV DEBIAN_FRONTEND=noninteractive

# 安装必要的包
RUN apt-get update && apt-get install -y \
    gcc \
    make \
    curl \
    iputils-ping \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制源代码
COPY . .

# 编译程序
RUN make clean && make all

# 创建启动脚本
RUN echo '#!/bin/bash\n\
echo "=== FaaSFlow Func Container in Docker ==="\n\
echo "Function ID: $1"\n\
echo "Proxy Socket: /tmp/faasflow_proxy.sock"\n\
\n\
# 等待Proxy启动\n\
echo "等待Proxy启动..."\n\
while [ ! -S /tmp/faasflow_proxy.sock ]; do\n\
    sleep 1\n\
done\n\
echo "Proxy已启动，开始连接..."\n\
\n\
# 启动func_container\n\
./func_container $1\n\
' > /app/start_container.sh

RUN chmod +x /app/start_container.sh

# 暴露端口（如果需要）
EXPOSE 8080

# 默认命令
CMD ["/app/start_container.sh", "1"]






