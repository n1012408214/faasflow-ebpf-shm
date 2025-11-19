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

// 外部声明共享内存管理函数
extern int init_shared_memory_pool();
extern int allocate_packet(uint32_t data_size, uint32_t function_id, packet_descriptor_t** desc, char** data_ptr);
extern char* get_packet_data(const packet_descriptor_t* desc);
extern int free_packet(uint64_t packet_id);
extern void notify_new_packet();
extern void cleanup_shared_memory();

// 配置
#define GATEWAY_PORT 8080
#define PROXY_PORT 8081
#define MAX_CLIENTS 100
#define BUFFER_SIZE 8192

// 全局变量
static int g_gateway_socket = -1;
static int g_proxy_socket = -1;
static volatile int g_running = 1;
static pthread_mutex_t g_stats_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint64_t g_total_requests = 0;
static uint64_t g_total_bytes = 0;

// 信号处理函数
static void signal_handler(int sig) {
    printf("\n[Gateway] 收到信号 %d，正在退出...\n", sig);
    g_running = 0;
}

// 连接到Proxy
static int connect_to_proxy() {
    printf("[Gateway] 连接到Proxy...\n");
    
    g_proxy_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (g_proxy_socket < 0) {
        perror("创建proxy socket失败");
        return -1;
    }
    
    struct sockaddr_in proxy_addr;
    memset(&proxy_addr, 0, sizeof(proxy_addr));
    proxy_addr.sin_family = AF_INET;
    proxy_addr.sin_port = htons(PROXY_PORT);
    proxy_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    
    if (connect(g_proxy_socket, (struct sockaddr*)&proxy_addr, sizeof(proxy_addr)) < 0) {
        perror("连接到proxy失败");
        close(g_proxy_socket);
        return -1;
    }
    
    printf("[Gateway] 成功连接到Proxy (端口: %d)\n", PROXY_PORT);
    return 0;
}

// 发送包描述符到Proxy
static int send_packet_descriptor_to_proxy(const packet_descriptor_t* desc) {
    if (!desc || g_proxy_socket < 0) {
        return -1;
    }
    
    ssize_t sent = send(g_proxy_socket, desc, sizeof(packet_descriptor_t), 0);
    if (sent != sizeof(packet_descriptor_t)) {
        perror("发送包描述符到proxy失败");
        return -1;
    }
    
    printf("[Gateway] 包描述符已发送到Proxy: ID=%lu, 函数ID=%u\n", 
           desc->packet_id, desc->function_id);
    return 0;
}

// 监听来自Proxy的响应
static void* listen_for_proxy_responses(void* arg) {
    printf("[Gateway] Proxy响应监听线程启动\n");
    
    while (g_running) {
        packet_descriptor_t output_desc;
        ssize_t received = recv(g_proxy_socket, &output_desc, sizeof(packet_descriptor_t), 0);
        
        if (received <= 0) {
            if (g_running) {
                printf("[Gateway] Proxy连接断开\n");
            }
            break;
        }
        
        if (received == sizeof(packet_descriptor_t)) {
            printf("[Gateway] 收到Proxy输出包描述符: ID=%lu, 函数ID=%u, 大小=%u\n", 
                   output_desc.packet_id, output_desc.function_id, output_desc.data_size);
            
            // 从共享内存读取输出数据
            char* output_data = get_packet_data(&output_desc);
            if (!output_data) {
                printf("[Gateway] 无法从共享内存读取输出数据\n");
                continue;
            }
            
            printf("[Gateway] 输出数据验证成功: %u 字节\n", output_desc.data_size);
            printf("[Gateway] 输出数据内容:\n%.*s\n", (int)output_desc.data_size, output_data);
            
            // 这里可以添加逻辑将结果返回给对应的客户端
            // 由于这是一个简单的demo，我们只打印结果
            
            // 释放输出包
            free_packet(output_desc.packet_id);
            
            printf("[Gateway] 输出包处理完成\n");
        } else {
            printf("[Gateway] 收到无效的包描述符大小: %ld\n", received);
        }
    }
    
    printf("[Gateway] Proxy响应监听线程退出\n");
    return NULL;
}

// 解析HTTP请求
static int parse_http_request(const char* request, char* method, char* path, uint32_t* function_id) {
    char buffer[BUFFER_SIZE];
    strncpy(buffer, request, BUFFER_SIZE - 1);
    buffer[BUFFER_SIZE - 1] = '\0';
    
    // 简单的HTTP解析
    char* line = strtok(buffer, "\n");
    if (!line) return -1;
    
    // 解析请求行: METHOD /path HTTP/1.1
    if (sscanf(line, "%s %s", method, path) != 2) {
        return -1;
    }
    
    // 从路径中提取函数ID: /function/{id}/...
    if (strncmp(path, "/function/", 10) == 0) {
        if (sscanf(path + 10, "%u", function_id) == 1) {
            return 0;
        }
    }
    
    // 默认函数ID为1
    *function_id = 1;
    return 0;
}

// 处理客户端请求
static void* handle_client(void* arg) {
    int client_socket = *(int*)arg;
    free(arg);
    
    char buffer[BUFFER_SIZE];
    ssize_t bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
    
    if (bytes_received > 0) {
        buffer[bytes_received] = '\0';
        
        printf("[Gateway] 收到HTTP请求 (%ld 字节):\n", bytes_received);
        printf("%s\n", buffer);
        
        // 解析HTTP请求
        char method[32], path[256];
        uint32_t function_id;
        
        if (parse_http_request(buffer, method, path, &function_id) == 0) {
            printf("[Gateway] 解析请求: 方法=%s, 路径=%s, 函数ID=%u\n", 
                   method, path, function_id);
            
            // 分配共享内存包
            packet_descriptor_t* desc;
            char* data_ptr;
            
            int ret = allocate_packet(bytes_received, function_id, &desc, &data_ptr);
            if (ret == SUCCESS) {
                // 复制数据到共享内存
                memcpy(data_ptr, buffer, bytes_received);
                
                // 发送包描述符到Proxy
                if (send_packet_descriptor_to_proxy(desc) == 0) {
                    // 通知新包到达
                    notify_new_packet();
                    
                    // 更新统计信息
                    pthread_mutex_lock(&g_stats_mutex);
                    g_total_requests++;
                    g_total_bytes += bytes_received;
                    pthread_mutex_unlock(&g_stats_mutex);
                    
                    // 发送HTTP响应
                    const char* response = "HTTP/1.1 200 OK\r\n"
                                          "Content-Type: text/plain\r\n"
                                          "Content-Length: 25\r\n"
                                          "\r\n"
                                          "Request queued successfully";
                    send(client_socket, response, strlen(response), 0);
                    
                    printf("[Gateway] 请求已排队: 包ID=%lu\n", desc->packet_id);
                } else {
                    // 发送错误响应
                    const char* error_response = "HTTP/1.1 500 Internal Server Error\r\n"
                                                "Content-Type: text/plain\r\n"
                                                "Content-Length: 20\r\n"
                                                "\r\n"
                                                "Internal server error";
                    send(client_socket, error_response, strlen(error_response), 0);
                    
                    // 释放包
                    free_packet(desc->packet_id);
                }
            } else {
                printf("[Gateway] 分配包失败: %s\n", get_error_string(ret));
                
                // 发送错误响应
                const char* error_response = "HTTP/1.1 503 Service Unavailable\r\n"
                                            "Content-Type: text/plain\r\n"
                                            "Content-Length: 20\r\n"
                                            "\r\n"
                                            "Service unavailable";
                send(client_socket, error_response, strlen(error_response), 0);
            }
        } else {
            printf("[Gateway] HTTP请求解析失败\n");
            
            // 发送400错误
            const char* error_response = "HTTP/1.1 400 Bad Request\r\n"
                                        "Content-Type: text/plain\r\n"
                                        "Content-Length: 15\r\n"
                                        "\r\n"
                                        "Bad request";
            send(client_socket, error_response, strlen(error_response), 0);
        }
    }
    
    close(client_socket);
    return NULL;
}

// 创建Gateway服务器
static int create_gateway_server() {
    printf("[Gateway] 创建Gateway服务器...\n");
    
    g_gateway_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (g_gateway_socket < 0) {
        perror("创建gateway socket失败");
        return -1;
    }
    
    int opt = 1;
    if (setsockopt(g_gateway_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt失败");
        close(g_gateway_socket);
        return -1;
    }
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(GATEWAY_PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    if (bind(g_gateway_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind失败");
        close(g_gateway_socket);
        return -1;
    }
    
    if (listen(g_gateway_socket, MAX_CLIENTS) < 0) {
        perror("listen失败");
        close(g_gateway_socket);
        return -1;
    }
    
    printf("[Gateway] Gateway服务器创建成功 (端口: %d)\n", GATEWAY_PORT);
    return 0;
}

// 打印统计信息
static void print_stats() {
    pthread_mutex_lock(&g_stats_mutex);
    printf("\n[Gateway] 统计信息:\n");
    printf("  总请求数: %lu\n", g_total_requests);
    printf("  总字节数: %lu\n", g_total_bytes);
    printf("  平均请求大小: %.2f 字节\n", 
           g_total_requests > 0 ? (double)g_total_bytes / g_total_requests : 0.0);
    pthread_mutex_unlock(&g_stats_mutex);
}

int main() {
    printf("=== FaaSFlow Gateway ===\n");
    printf("端口: %d\n", GATEWAY_PORT);
    printf("Proxy端口: %d\n", PROXY_PORT);
    
    // 设置信号处理
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // 初始化共享内存池
    if (init_shared_memory_pool() != SUCCESS) {
        fprintf(stderr, "错误: 共享内存池初始化失败\n");
        return 1;
    }
    
    // 连接到Proxy
    if (connect_to_proxy() != 0) {
        fprintf(stderr, "错误: 无法连接到Proxy\n");
        cleanup_shared_memory();
        return 1;
    }
    
    // 创建Gateway服务器
    if (create_gateway_server() != 0) {
        fprintf(stderr, "错误: Gateway服务器创建失败\n");
        cleanup_shared_memory();
        return 1;
    }
    
    printf("[Gateway] Gateway已启动，等待客户端连接...\n");
    printf("测试命令: curl -X POST http://localhost:%d/function/1 -d 'Hello World'\n", GATEWAY_PORT);
    
    // 创建Proxy响应监听线程
    pthread_t proxy_response_thread;
    if (pthread_create(&proxy_response_thread, NULL, listen_for_proxy_responses, NULL) != 0) {
        perror("创建Proxy响应监听线程失败");
        cleanup_shared_memory();
        return 1;
    }
    
    // 主循环：接受客户端连接
    while (g_running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_socket = accept(g_gateway_socket, (struct sockaddr*)&client_addr, &client_len);
        if (client_socket < 0) {
            if (g_running) {
                perror("accept失败");
            }
            continue;
        }
        
        printf("[Gateway] 新客户端连接: %s:%d\n", 
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        
        // 创建线程处理客户端请求
        int* client_sock_ptr = malloc(sizeof(int));
        *client_sock_ptr = client_socket;
        
        pthread_t thread;
        if (pthread_create(&thread, NULL, handle_client, client_sock_ptr) != 0) {
            perror("创建客户端处理线程失败");
            free(client_sock_ptr);
            close(client_socket);
        } else {
            pthread_detach(thread);
        }
    }
    
    printf("[Gateway] 正在清理资源...\n");
    
    // 清理资源
    if (g_gateway_socket >= 0) {
        close(g_gateway_socket);
    }
    
    if (g_proxy_socket >= 0) {
        close(g_proxy_socket);
    }
    
    cleanup_shared_memory();
    
    print_stats();
    printf("[Gateway] Gateway已退出\n");
    
    return 0;
}
