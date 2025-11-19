#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <time.h>

// 外部声明共享内存管理函数
extern int init_shared_memory_pool();
extern packet_descriptor_t* get_packet_descriptor(uint64_t packet_id);
extern char* get_packet_data(const packet_descriptor_t* desc);
extern int allocate_packet(uint32_t data_size, uint32_t function_id, packet_descriptor_t** desc, char** data_ptr);
extern int free_packet(uint64_t packet_id);
extern void cleanup_shared_memory();

// 配置
#define PROXY_SOCKET_PATH "/tmp/faasflow_proxy.sock"
#define MAX_PACKET_SIZE (64 * 1024)
#define BUFFER_SIZE 8192

// 全局变量
static int g_proxy_socket = -1;
static uint32_t g_function_id = 1;
static volatile int g_running = 1;
static uint64_t g_total_requests = 0;
static uint64_t g_total_bytes = 0;
static uint64_t g_total_processing_time = 0;

// 信号处理函数
static void signal_handler(int sig) {
    printf("\n[Container] 收到信号 %d，正在退出...\n", sig);
    g_running = 0;
}

// 连接到Proxy
static int connect_to_proxy() {
    printf("[Container] 连接到Proxy...\n");
    
    g_proxy_socket = socket(AF_UNIX, SOCK_STREAM, 0);
    if (g_proxy_socket < 0) {
        perror("创建proxy socket失败");
        return -1;
    }
    
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, PROXY_SOCKET_PATH);
    
    if (connect(g_proxy_socket, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("连接到proxy失败");
        close(g_proxy_socket);
        return -1;
    }
    
    printf("[Container] 成功连接到Proxy\n");
    
    // 注册容器信息
    char register_msg[256];
    snprintf(register_msg, sizeof(register_msg), "REGISTER:%u", g_function_id);
    
    ssize_t sent = send(g_proxy_socket, register_msg, strlen(register_msg), 0);
    if (sent < 0) {
        perror("发送注册信息失败");
        close(g_proxy_socket);
        return -1;
    }
    
    // 等待确认
    char ack[32];
    ssize_t received = recv(g_proxy_socket, ack, sizeof(ack) - 1, 0);
    if (received > 0) {
        ack[received] = '\0';
        if (strcmp(ack, "OK") == 0) {
            printf("[Container] 注册成功: 函数ID=%u\n", g_function_id);
            return 0;
        }
    }
    
    printf("[Container] 注册失败\n");
    close(g_proxy_socket);
    return -1;
}

// 示例函数：简单的字符串处理
static char* process_function_data(const char* input_data, size_t data_size, size_t* output_size) {
    printf("[Container] 处理函数数据: %zu 字节\n", data_size);
    
    // 简单的处理逻辑：将输入转换为大写并添加时间戳
    char* output = malloc(data_size + 256);  // 额外空间用于时间戳
    if (!output) {
        *output_size = 0;
        return NULL;
    }
    
    // 复制输入数据
    memcpy(output, input_data, data_size);
    output[data_size] = '\0';
    
    // 转换为大写
    for (size_t i = 0; i < data_size; i++) {
        if (output[i] >= 'a' && output[i] <= 'z') {
            output[i] = output[i] - 'a' + 'A';
        }
    }
    
    // 添加时间戳
    time_t now = time(NULL);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", localtime(&now));
    
    snprintf(output + data_size, 256, "\n[处理时间: %s]\n[函数ID: %u]", timestamp, g_function_id);
    
    *output_size = strlen(output);
    return output;
}

// 发送处理结果回Proxy
static int send_result_to_proxy(const packet_descriptor_t* input_desc, const char* output_data, size_t output_size) {
    printf("[Container] 发送处理结果回Proxy...\n");
    
    // 分配共享内存包用于输出数据
    packet_descriptor_t* output_desc;
    char* output_ptr;
    
    if (allocate_packet(output_size, g_function_id, &output_desc, &output_ptr) != SUCCESS) {
        printf("[Container] 分配输出包失败\n");
        return -1;
    }
    
    // 复制输出数据到共享内存
    memcpy(output_ptr, output_data, output_size);
    
    // 设置输出包描述符
    output_desc->packet_id = input_desc->packet_id + 1000000;  // 输出包ID = 输入包ID + 1000000
    output_desc->function_id = g_function_id;
    output_desc->data_size = output_size;
    output_desc->timestamp = get_timestamp();
    output_desc->flags = PACKET_FLAG_VALID | PACKET_FLAG_PROCESSED;
    
    printf("[Container] 输出包已写入共享内存: ID=%lu, 大小=%u\n", 
           output_desc->packet_id, output_desc->data_size);
    
    // 发送输出包描述符回Proxy
    ssize_t sent = send(g_proxy_socket, output_desc, sizeof(packet_descriptor_t), 0);
    if (sent != sizeof(packet_descriptor_t)) {
        printf("[Container] 发送输出包描述符失败: %ld\n", sent);
        free_packet(output_desc->packet_id);
        return -1;
    }
    
    printf("[Container] 输出包描述符已发送回Proxy\n");
    return 0;
}

// 处理包
static int process_packet(const packet_descriptor_t* desc) {
    if (!desc) return -1;
    
    printf("[Container] 处理包: ID=%lu, 大小=%u\n", desc->packet_id, desc->data_size);
    
    // 获取包数据
    char* data = get_packet_data(desc);
    if (!data) {
        printf("[Container] 无法获取包数据\n");
        return -1;
    }
    
    // 记录开始时间
    uint64_t start_time = get_timestamp();
    
    // 处理数据
    size_t output_size;
    char* output = process_function_data(data, desc->data_size, &output_size);
    
    if (output) {
        // 记录处理时间
        uint64_t end_time = get_timestamp();
        uint64_t processing_time = end_time - start_time;
        
        printf("[Container] 处理完成: 输出大小=%zu, 处理时间=%lu 微秒\n", 
               output_size, processing_time);
        
        // 发送处理结果回Proxy
        int result = send_result_to_proxy(desc, output, output_size);
        
        // 更新统计信息
        g_total_requests++;
        g_total_bytes += desc->data_size;
        g_total_processing_time += processing_time;
        
        free(output);
        
        // 释放输入包
        free_packet(desc->packet_id);
        
        return result;
    } else {
        printf("[Container] 处理失败\n");
        return -1;
    }
}

// 接收和处理包
static void* receive_and_process_packets(void* arg) {
    printf("[Container] 包处理线程启动\n");
    
    while (g_running) {
        // 接收包描述符
        packet_descriptor_t desc;
        ssize_t received = recv(g_proxy_socket, &desc, sizeof(desc), 0);
        
        if (received <= 0) {
            if (g_running) {
                printf("[Container] Proxy连接断开\n");
            }
            break;
        }
        
        if (received == sizeof(packet_descriptor_t)) {
            printf("[Container] 收到包描述符: ID=%lu, 函数ID=%u, 大小=%u\n", 
                   desc.packet_id, desc.function_id, desc.data_size);
            
            // 验证函数ID
            if (desc.function_id != g_function_id) {
                printf("[Container] 函数ID不匹配: 期望=%u, 收到=%u\n", 
                       g_function_id, desc.function_id);
                continue;
            }
            
            // 处理包（数据已经在共享内存中）
            process_packet(&desc);
        } else {
            printf("[Container] 收到无效的包描述符大小: %ld\n", received);
        }
    }
    
    printf("[Container] 包处理线程退出\n");
    return NULL;
}

// 打印统计信息
static void print_stats() {
    printf("\n[Container] 统计信息:\n");
    printf("  函数ID: %u\n", g_function_id);
    printf("  总请求数: %lu\n", g_total_requests);
    printf("  总字节数: %lu\n", g_total_bytes);
    if (g_total_requests > 0) {
        printf("  平均请求大小: %.2f 字节\n", (double)g_total_bytes / g_total_requests);
        printf("  平均处理时间: %.2f 微秒\n", (double)g_total_processing_time / g_total_requests);
    }
}

// 显示使用说明
static void show_usage(const char* program_name) {
    printf("用法: %s [函数ID]\n", program_name);
    printf("  函数ID: 容器处理的函数ID (默认: 1)\n");
    printf("\n示例:\n");
    printf("  %s 1     # 启动函数ID为1的容器\n", program_name);
    printf("  %s 2     # 启动函数ID为2的容器\n", program_name);
}

int main(int argc, char* argv[]) {
    // 解析命令行参数
    if (argc > 1) {
        if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
            show_usage(argv[0]);
            return 0;
        }
        
        g_function_id = atoi(argv[1]);
        if (g_function_id == 0) {
            fprintf(stderr, "错误: 无效的函数ID: %s\n", argv[1]);
            show_usage(argv[0]);
            return 1;
        }
    }
    
    printf("=== FaaSFlow Func Container ===\n");
    printf("函数ID: %u\n", g_function_id);
    printf("Proxy Socket: %s\n", PROXY_SOCKET_PATH);
    
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
    
    printf("[Container] 容器已启动，等待包...\n");
    
    // 创建包处理线程
    pthread_t process_thread;
    if (pthread_create(&process_thread, NULL, receive_and_process_packets, NULL) != 0) {
        perror("创建包处理线程失败");
        cleanup_shared_memory();
        return 1;
    }
    
    // 等待线程结束
    pthread_join(process_thread, NULL);
    
    printf("[Container] 正在清理资源...\n");
    
    // 清理资源
    if (g_proxy_socket >= 0) {
        close(g_proxy_socket);
    }
    
    cleanup_shared_memory();
    
    print_stats();
    printf("[Container] 容器已退出\n");
    
    return 0;
}
