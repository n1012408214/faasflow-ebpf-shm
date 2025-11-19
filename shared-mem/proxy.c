#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <sys/un.h>

// 外部声明共享内存管理函数
extern int init_shared_memory_pool();
extern packet_descriptor_t* get_packet_descriptor(uint64_t packet_id);
extern char* get_packet_data(const packet_descriptor_t* desc);
extern int wait_for_packet(uint64_t timeout_ms);
extern void cleanup_shared_memory();

// 前向声明
static int send_packet_to_container(const packet_descriptor_t* desc);

// 配置
#define PROXY_PORT 8081
#define FUNC_CONTAINER_PORT 8082
#define MAX_CONTAINERS 10
#define MAX_CONTAINER_CONNECTIONS 5

// 容器连接信息
typedef struct {
    int socket_fd;
    uint32_t function_id;
    int active;
    pthread_mutex_t mutex;
} container_connection_t;

// 全局变量
static int g_proxy_socket = -1;
static int g_gateway_socket = -1;
static volatile int g_running = 1;
static container_connection_t g_containers[MAX_CONTAINERS];
static pthread_mutex_t g_containers_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint64_t g_total_packets = 0;
static uint64_t g_total_bytes = 0;

// 信号处理函数
static void signal_handler(int sig) {
    printf("\n[Proxy] 收到信号 %d，正在退出...\n", sig);
    g_running = 0;
}

// 创建Proxy服务器
static int create_proxy_server() {
    printf("[Proxy] 创建Proxy服务器...\n");
    
    g_proxy_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (g_proxy_socket < 0) {
        perror("创建proxy socket失败");
        return -1;
    }
    
    int opt = 1;
    if (setsockopt(g_proxy_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt失败");
        close(g_proxy_socket);
        return -1;
    }
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PROXY_PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    if (bind(g_proxy_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind失败");
        close(g_proxy_socket);
        return -1;
    }
    
    if (listen(g_proxy_socket, MAX_CONTAINERS) < 0) {
        perror("listen失败");
        close(g_proxy_socket);
        return -1;
    }
    
    printf("[Proxy] Proxy服务器创建成功 (端口: %d)\n", PROXY_PORT);
    return 0;
}

// 处理来自Gateway的连接
static void* handle_gateway_connection(void* arg) {
    printf("[Proxy] Gateway连接处理线程启动\n");
    
    while (g_running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_socket = accept(g_proxy_socket, (struct sockaddr*)&client_addr, &client_len);
        if (client_socket < 0) {
            if (g_running) {
                perror("accept gateway连接失败");
            }
            continue;
        }
        
        printf("[Proxy] Gateway连接: %s:%d\n", 
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        
        g_gateway_socket = client_socket;
        
        // 接收包描述符
        while (g_running) {
            packet_descriptor_t desc;
            ssize_t received = recv(g_gateway_socket, &desc, sizeof(desc), 0);
            
            if (received <= 0) {
                if (g_running) {
                    printf("[Proxy] Gateway连接断开\n");
                }
                break;
            }
            
            if (received == sizeof(packet_descriptor_t)) {
                printf("[Proxy] 收到包描述符: ID=%lu, 函数ID=%u, 大小=%u\n", 
                       desc.packet_id, desc.function_id, desc.data_size);
                
                // 发送包描述符到对应的容器
                if (send_packet_to_container(&desc) == 0) {
                    g_total_packets++;
                    g_total_bytes += desc.data_size;
                    printf("[Proxy] 包已发送到容器: ID=%lu\n", desc.packet_id);
                } else {
                    printf("[Proxy] 发送包到容器失败: ID=%lu\n", desc.packet_id);
                }
            } else {
                printf("[Proxy] 收到无效的包描述符大小: %ld\n", received);
            }
        }
        
        close(g_gateway_socket);
        g_gateway_socket = -1;
    }
    
    printf("[Proxy] Gateway连接处理线程退出\n");
    return NULL;
}

// 处理容器连接
static void* handle_container_connection(void* arg) {
    int container_socket = *(int*)arg;
    free(arg);
    
    // 接收容器注册信息
    char buffer[256];
    ssize_t received = recv(container_socket, buffer, sizeof(buffer) - 1, 0);
    if (received > 0) {
        buffer[received] = '\0';
        
        // 解析容器信息: "REGISTER:function_id"
        uint32_t function_id;
        if (sscanf(buffer, "REGISTER:%u", &function_id) == 1) {
            printf("[Proxy] 容器注册: 函数ID=%u, Socket=%d\n", function_id, container_socket);
            
            // 注册容器连接
            pthread_mutex_lock(&g_containers_mutex);
            for (int i = 0; i < MAX_CONTAINERS; i++) {
                if (!g_containers[i].active) {
                    g_containers[i].socket_fd = container_socket;
                    g_containers[i].function_id = function_id;
                    g_containers[i].active = 1;
                    pthread_mutex_init(&g_containers[i].mutex, NULL);
                    break;
                }
            }
            pthread_mutex_unlock(&g_containers_mutex);
            
            // 发送确认
            const char* ack = "OK";
            send(container_socket, ack, strlen(ack), 0);
            
            printf("[Proxy] 容器注册成功: 函数ID=%u\n", function_id);
            
            // 开始监听来自容器的包描述符
            while (g_running) {
                packet_descriptor_t output_desc;
                ssize_t desc_received = recv(container_socket, &output_desc, sizeof(packet_descriptor_t), 0);
                
                if (desc_received <= 0) {
                    if (g_running) {
                        printf("[Proxy] 容器连接断开: 函数ID=%u\n", function_id);
                    }
                    break;
                }
                
                if (desc_received == sizeof(packet_descriptor_t)) {
                    printf("[Proxy] 收到容器输出包描述符: ID=%lu, 函数ID=%u, 大小=%u\n", 
                           output_desc.packet_id, output_desc.function_id, output_desc.data_size);
                    
                    // 验证包描述符
                    if (output_desc.function_id != function_id) {
                        printf("[Proxy] 包描述符函数ID不匹配: 期望=%u, 收到=%u\n", 
                               function_id, output_desc.function_id);
                        continue;
                    }
                    
                    // 验证共享内存中的数据
                    char* output_data = get_packet_data(&output_desc);
                    if (!output_data) {
                        printf("[Proxy] 无法从共享内存读取输出数据\n");
                        continue;
                    }
                    
                    printf("[Proxy] 输出数据验证成功: %u 字节\n", output_desc.data_size);
                    printf("[Proxy] 输出数据预览: %.50s...\n", output_data);
                    
                    // 转发包描述符给Gateway
                    if (g_gateway_socket >= 0) {
                        ssize_t sent = send(g_gateway_socket, &output_desc, sizeof(packet_descriptor_t), 0);
                        if (sent == sizeof(packet_descriptor_t)) {
                            printf("[Proxy] 输出包描述符已转发给Gateway\n");
                            
                            // 更新统计信息
                            g_total_packets++;
                            g_total_bytes += output_desc.data_size;
                        } else {
                            printf("[Proxy] 转发输出包描述符给Gateway失败: %ld\n", sent);
                        }
                    } else {
                        printf("[Proxy] Gateway连接不可用，无法转发输出包描述符\n");
                    }
                } else {
                    printf("[Proxy] 收到无效的包描述符大小: %ld\n", desc_received);
                }
            }
            
            // 清理容器连接
            pthread_mutex_lock(&g_containers_mutex);
            for (int i = 0; i < MAX_CONTAINERS; i++) {
                if (g_containers[i].active && g_containers[i].function_id == function_id) {
                    g_containers[i].active = 0;
                    pthread_mutex_destroy(&g_containers[i].mutex);
                    break;
                }
            }
            pthread_mutex_unlock(&g_containers_mutex);
            
        } else {
            printf("[Proxy] 无效的容器注册信息: %s\n", buffer);
            close(container_socket);
        }
    } else {
        printf("[Proxy] 容器连接失败\n");
        close(container_socket);
    }
    
    return NULL;
}

// 发送包到容器
static int send_packet_to_container(const packet_descriptor_t* desc) {
    if (!desc) return -1;
    
    pthread_mutex_lock(&g_containers_mutex);
    
    // 查找对应的容器
    container_connection_t* container = NULL;
    for (int i = 0; i < MAX_CONTAINERS; i++) {
        if (g_containers[i].active && g_containers[i].function_id == desc->function_id) {
            container = &g_containers[i];
            break;
        }
    }
    
    if (!container) {
        pthread_mutex_unlock(&g_containers_mutex);
        printf("[Proxy] 未找到函数ID=%u的容器\n", desc->function_id);
        return -1;
    }
    
    pthread_mutex_lock(&container->mutex);
    pthread_mutex_unlock(&g_containers_mutex);
    
    // 发送包描述符到容器（数据已经在共享内存中）
    ssize_t sent = send(container->socket_fd, desc, sizeof(packet_descriptor_t), 0);
    if (sent != sizeof(packet_descriptor_t)) {
        pthread_mutex_unlock(&container->mutex);
        printf("[Proxy] 发送包描述符到容器失败\n");
        return -1;
    }
    
    printf("[Proxy] 包描述符已发送到容器: ID=%lu, 函数ID=%u\n", 
           desc->packet_id, desc->function_id);
    
    pthread_mutex_unlock(&container->mutex);
    return 0;
}

// 监听容器连接
static void* listen_for_containers(void* arg) {
    printf("[Proxy] 容器监听线程启动\n");
    
    // 创建Unix域socket监听容器连接
    int container_socket = socket(AF_UNIX, SOCK_STREAM, 0);
    if (container_socket < 0) {
        perror("创建容器socket失败");
        return NULL;
    }
    
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, "/tmp/faasflow_proxy.sock");
    
    // 删除可能存在的socket文件
    unlink(addr.sun_path);
    
    if (bind(container_socket, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind容器socket失败");
        close(container_socket);
        return NULL;
    }
    
    if (listen(container_socket, MAX_CONTAINERS) < 0) {
        perror("listen容器socket失败");
        close(container_socket);
        return NULL;
    }
    
    printf("[Proxy] 容器监听socket创建成功: %s\n", addr.sun_path);
    
    while (g_running) {
        int client_socket = accept(container_socket, NULL, NULL);
        if (client_socket < 0) {
            if (g_running) {
                perror("accept容器连接失败");
            }
            continue;
        }
        
        printf("[Proxy] 新容器连接: Socket=%d\n", client_socket);
        
        // 创建线程处理容器连接
        int* client_sock_ptr = malloc(sizeof(int));
        *client_sock_ptr = client_socket;
        
        pthread_t thread;
        if (pthread_create(&thread, NULL, handle_container_connection, client_sock_ptr) != 0) {
            perror("创建容器处理线程失败");
            free(client_sock_ptr);
            close(client_socket);
        } else {
            pthread_detach(thread);
        }
    }
    
    close(container_socket);
    unlink(addr.sun_path);
    
    printf("[Proxy] 容器监听线程退出\n");
    return NULL;
}

// 打印统计信息
static void print_stats() {
    printf("\n[Proxy] 统计信息:\n");
    printf("  总包数: %lu\n", g_total_packets);
    printf("  总字节数: %lu\n", g_total_bytes);
    printf("  活跃容器数: ");
    
    int active_count = 0;
    pthread_mutex_lock(&g_containers_mutex);
    for (int i = 0; i < MAX_CONTAINERS; i++) {
        if (g_containers[i].active) {
            active_count++;
            printf("%u ", g_containers[i].function_id);
        }
    }
    pthread_mutex_unlock(&g_containers_mutex);
    
    if (active_count == 0) {
        printf("无");
    }
    printf("\n");
}

int main() {
    printf("=== FaaSFlow Proxy ===\n");
    printf("端口: %d\n", PROXY_PORT);
    
    // 设置信号处理
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // 初始化共享内存池
    if (init_shared_memory_pool() != SUCCESS) {
        fprintf(stderr, "错误: 共享内存池初始化失败\n");
        return 1;
    }
    
    // 初始化容器连接数组
    memset(g_containers, 0, sizeof(g_containers));
    
    // 创建Proxy服务器
    if (create_proxy_server() != 0) {
        fprintf(stderr, "错误: Proxy服务器创建失败\n");
        cleanup_shared_memory();
        return 1;
    }
    
    printf("[Proxy] Proxy已启动，等待连接...\n");
    
    // 创建Gateway连接处理线程
    pthread_t gateway_thread;
    if (pthread_create(&gateway_thread, NULL, handle_gateway_connection, NULL) != 0) {
        perror("创建Gateway处理线程失败");
        cleanup_shared_memory();
        return 1;
    }
    
    // 创建容器监听线程
    pthread_t container_thread;
    if (pthread_create(&container_thread, NULL, listen_for_containers, NULL) != 0) {
        perror("创建容器监听线程失败");
        cleanup_shared_memory();
        return 1;
    }
    
    // 等待线程结束
    pthread_join(gateway_thread, NULL);
    pthread_join(container_thread, NULL);
    
    printf("[Proxy] 正在清理资源...\n");
    
    // 清理容器连接
    pthread_mutex_lock(&g_containers_mutex);
    for (int i = 0; i < MAX_CONTAINERS; i++) {
        if (g_containers[i].active) {
            close(g_containers[i].socket_fd);
            pthread_mutex_destroy(&g_containers[i].mutex);
        }
    }
    pthread_mutex_unlock(&g_containers_mutex);
    
    // 清理资源
    if (g_proxy_socket >= 0) {
        close(g_proxy_socket);
    }
    
    if (g_gateway_socket >= 0) {
        close(g_gateway_socket);
    }
    
    cleanup_shared_memory();
    
    print_stats();
    printf("[Proxy] Proxy已退出\n");
    
    return 0;
}
