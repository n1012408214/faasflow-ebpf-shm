CC = gcc
CFLAGS = -O2 -g -Wall -Wextra -std=c99 -D_POSIX_C_SOURCE=200809L -fPIC
LDFLAGS = -lpthread -lrt

# 目标文件
TARGETS = gateway proxy func_container
OBJECTS = shm_manager.o
SHARED_LIB = libfaasflow_shm.so

# 默认目标
all: $(TARGETS) $(SHARED_LIB)

# 编译共享内存管理库
shm_manager.o: shm_manager.c common.h
	$(CC) $(CFLAGS) -c shm_manager.c -o shm_manager.o

# 编译共享库
$(SHARED_LIB): shm_manager.o
	$(CC) -shared -o $(SHARED_LIB) shm_manager.o $(LDFLAGS)

# 编译Gateway
gateway: gateway.c shm_manager.o common.h
	$(CC) $(CFLAGS) gateway.c shm_manager.o -o gateway $(LDFLAGS)

# 编译Proxy
proxy: proxy.c shm_manager.o common.h
	$(CC) $(CFLAGS) proxy.c shm_manager.o -o proxy $(LDFLAGS)

# 编译Func Container
func_container: func_container.c shm_manager.o common.h
	$(CC) $(CFLAGS) func_container.c shm_manager.o -o func_container $(LDFLAGS)

# 清理
clean:
	rm -f $(TARGETS) $(OBJECTS) $(SHARED_LIB)
	rm -f /tmp/faasflow_proxy.sock

# 安装共享库
install: $(SHARED_LIB)
	@echo "安装共享内存库..."
	sudo cp $(SHARED_LIB) /usr/local/lib/
	sudo ldconfig
	@echo "共享库已安装到 /usr/local/lib/$(SHARED_LIB)"

# 运行测试
test: all
	@echo "启动共享内存架构测试..."
	@echo "1. 启动Proxy..."
	@./proxy &
	@sleep 2
	@echo "2. 启动Func Container (函数ID=1)..."
	@./func_container 1 &
	@sleep 2
	@echo "3. 启动Gateway..."
	@./gateway &
	@sleep 2
	@echo "4. 测试HTTP请求..."
	@curl -X POST http://localhost:8080/function/1 -d "Hello World from FaaSFlow!"
	@echo ""
	@echo "5. 清理进程..."
	@pkill -f gateway
	@pkill -f proxy
	@pkill -f func_container
	@echo "测试完成"

# 显示帮助
help:
	@echo "FaaSFlow 共享内存架构 Makefile"
	@echo ""
	@echo "可用目标:"
	@echo "  all          - 编译所有组件和共享库"
	@echo "  gateway      - 编译Gateway组件"
	@echo "  proxy        - 编译Proxy组件"
	@echo "  func_container - 编译Func Container组件"
	@echo "  libfaasflow_shm.so - 编译共享内存库"
	@echo "  install      - 安装共享库到系统"
	@echo "  clean        - 清理编译文件"
	@echo "  test         - 运行完整测试"
	@echo "  help         - 显示此帮助"
	@echo ""
	@echo "使用示例:"
	@echo "  make all                    # 编译所有组件和共享库"
	@echo "  make install                # 安装共享库"
	@echo "  make test                   # 运行完整测试"
	@echo "  ./gateway &                 # 启动Gateway"
	@echo "  ./proxy &                   # 启动Proxy"
	@echo "  ./func_container 1 &        # 启动函数ID=1的容器"
	@echo "  curl -X POST http://localhost:8080/function/1 -d 'Hello'"

.PHONY: all clean install test help
