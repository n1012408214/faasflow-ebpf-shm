#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>

// 全局共享内存池指针
static shared_memory_pool_t* g_shm_pool = NULL;
static int g_shm_fd = -1;

// 获取当前时间戳（微秒）
uint64_t get_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000 + (uint64_t)ts.tv_nsec / 1000;
}

// 初始化共享内存池
int init_shared_memory_pool() {
    printf("[SHM] 初始化共享内存池...\n");
    
    // 创建或打开共享内存
    g_shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (g_shm_fd == -1) {
        perror("shm_open失败");
        return ERROR_SHM_INIT;
    }
    
    // 设置共享内存大小
    if (ftruncate(g_shm_fd, SHM_SIZE) == -1) {
        perror("ftruncate失败");
        close(g_shm_fd);
        return ERROR_SHM_INIT;
    }
    
    // 映射共享内存
    g_shm_pool = mmap(NULL, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, g_shm_fd, 0);
    if (g_shm_pool == MAP_FAILED) {
        perror("mmap失败");
        close(g_shm_fd);
        return ERROR_SHM_INIT;
    }
    
    // 检查是否是新创建的共享内存
    if (g_shm_pool->header.magic != SHM_MAGIC) {
        printf("[SHM] 初始化新的共享内存池...\n");
        
        // 初始化头部
        g_shm_pool->header.magic = SHM_MAGIC;
        g_shm_pool->header.version = SHM_VERSION;
        g_shm_pool->header.total_size = SHM_SIZE;
        g_shm_pool->header.data_offset = sizeof(shared_memory_pool_t);
        g_shm_pool->header.data_size = SHM_SIZE - sizeof(shared_memory_pool_t);
        g_shm_pool->header.packet_count = 0;
        g_shm_pool->header.next_packet_id = 1;
        
        // 初始化互斥锁和条件变量
        pthread_mutexattr_t mutex_attr;
        pthread_mutexattr_init(&mutex_attr);
        pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
        pthread_mutex_init(&g_shm_pool->header.mutex, &mutex_attr);
        pthread_mutexattr_destroy(&mutex_attr);
        
        pthread_condattr_t cond_attr;
        pthread_condattr_init(&cond_attr);
        pthread_condattr_setpshared(&cond_attr, PTHREAD_PROCESS_SHARED);
        pthread_cond_init(&g_shm_pool->header.cond, &cond_attr);
        pthread_condattr_destroy(&cond_attr);
        
        // 初始化包描述符数组
        memset(g_shm_pool->descriptors, 0, sizeof(g_shm_pool->descriptors));
        
        printf("[SHM] 共享内存池初始化完成\n");
    } else {
        printf("[SHM] 连接到现有共享内存池\n");
    }
    
    printf("[SHM] 共享内存池信息:\n");
    printf("  总大小: %u 字节\n", g_shm_pool->header.total_size);
    printf("  数据区域: %u 字节\n", g_shm_pool->header.data_size);
    printf("  当前包数: %u\n", g_shm_pool->header.packet_count);
    printf("  下一个包ID: %u\n", g_shm_pool->header.next_packet_id);
    
    return SUCCESS;
}

// 分配包描述符和数据空间
int allocate_packet(uint32_t data_size, uint32_t function_id, packet_descriptor_t** desc, char** data_ptr) {
    if (data_size > MAX_PACKET_SIZE) {
        printf("[SHM] 数据大小超过限制: %u > %u\n", data_size, MAX_PACKET_SIZE);
        return ERROR_INVALID_PACKET;
    }
    
    pthread_mutex_lock(&g_shm_pool->header.mutex);
    
    // 检查包数量限制
    if (g_shm_pool->header.packet_count >= MAX_PACKETS) {
        pthread_mutex_unlock(&g_shm_pool->header.mutex);
        printf("[SHM] 包数量已达上限: %u\n", MAX_PACKETS);
        return ERROR_PACKET_FULL;
    }
    
    // 查找空闲的包描述符
    int desc_index = -1;
    for (int i = 0; i < MAX_PACKETS; i++) {
        if (!(g_shm_pool->descriptors[i].flags & PACKET_FLAG_VALID)) {
            desc_index = i;
            break;
        }
    }
    
    if (desc_index == -1) {
        pthread_mutex_unlock(&g_shm_pool->header.mutex);
        printf("[SHM] 没有可用的包描述符\n");
        return ERROR_PACKET_FULL;
    }
    
    // 分配数据空间（使用描述符索引计算偏移）
    uint32_t data_offset = g_shm_pool->header.data_offset + 
                          (desc_index * MAX_PACKET_SIZE);
    
    // 检查数据区域是否足够
    if (data_offset + data_size > g_shm_pool->header.data_offset + g_shm_pool->header.data_size) {
        pthread_mutex_unlock(&g_shm_pool->header.mutex);
        printf("[SHM] 数据区域空间不足\n");
        return ERROR_PACKET_FULL;
    }
    
    // 初始化包描述符
    packet_descriptor_t* packet_desc = &g_shm_pool->descriptors[desc_index];
    packet_desc->packet_id = g_shm_pool->header.next_packet_id++;
    packet_desc->data_offset = data_offset;
    packet_desc->data_size = data_size;
    packet_desc->function_id = function_id;
    packet_desc->timestamp = get_timestamp();
    packet_desc->flags = PACKET_FLAG_VALID;
    memset(packet_desc->reserved, 0, sizeof(packet_desc->reserved));
    
    // 更新包数量
    g_shm_pool->header.packet_count++;
    
    // 设置返回指针
    *desc = packet_desc;
    *data_ptr = (char*)g_shm_pool + data_offset;
    
    pthread_mutex_unlock(&g_shm_pool->header.mutex);
    
    printf("[SHM] 分配包成功: ID=%lu, 大小=%u, 函数ID=%u\n", 
           packet_desc->packet_id, data_size, function_id);
    
    return SUCCESS;
}

// 释放包描述符
int free_packet(uint64_t packet_id) {
    pthread_mutex_lock(&g_shm_pool->header.mutex);
    
    // 查找包描述符
    int desc_index = -1;
    for (int i = 0; i < MAX_PACKETS; i++) {
        if ((g_shm_pool->descriptors[i].flags & PACKET_FLAG_VALID) && 
            g_shm_pool->descriptors[i].packet_id == packet_id) {
            desc_index = i;
            break;
        }
    }
    
    if (desc_index == -1) {
        pthread_mutex_unlock(&g_shm_pool->header.mutex);
        printf("[SHM] 包不存在: ID=%lu\n", packet_id);
        return ERROR_INVALID_PACKET;
    }
    
    // 清除包描述符
    memset(&g_shm_pool->descriptors[desc_index], 0, sizeof(packet_descriptor_t));
    g_shm_pool->header.packet_count--;
    
    pthread_mutex_unlock(&g_shm_pool->header.mutex);
    
    printf("[SHM] 释放包成功: ID=%lu\n", packet_id);
    return SUCCESS;
}

// 获取包描述符
packet_descriptor_t* get_packet_descriptor(uint64_t packet_id) {
    if (!g_shm_pool) {
        printf("[SHM] get_packet_descriptor: 共享内存池未初始化\n");
        return NULL;
    }
    
    pthread_mutex_lock(&g_shm_pool->header.mutex);
    
    for (int i = 0; i < MAX_PACKETS; i++) {
        if ((g_shm_pool->descriptors[i].flags & PACKET_FLAG_VALID) && 
            g_shm_pool->descriptors[i].packet_id == packet_id) {
            printf("[SHM] get_packet_descriptor: 找到包ID=%lu, 索引=%d\n", packet_id, i);
            pthread_mutex_unlock(&g_shm_pool->header.mutex);
            return &g_shm_pool->descriptors[i];
        }
    }
    
    printf("[SHM] get_packet_descriptor: 未找到包ID=%lu\n", packet_id);
    pthread_mutex_unlock(&g_shm_pool->header.mutex);
    return NULL;
}

// 获取包数据指针
char* get_packet_data(const packet_descriptor_t* desc) {
    if (!desc || !(desc->flags & PACKET_FLAG_VALID) || !g_shm_pool) {
        printf("[SHM] get_packet_data: 无效参数\n");
        return NULL;
    }
    
    printf("[SHM] get_packet_data: 包ID=%lu, 偏移=%u, 大小=%u\n", 
           desc->packet_id, desc->data_offset, desc->data_size);
    
    char* data_ptr = (char*)g_shm_pool + desc->data_offset;
    printf("[SHM] get_packet_data: 数据指针=%p\n", (void*)data_ptr);
    
    return data_ptr;
}

// 等待新包
int wait_for_packet(uint64_t timeout_ms) {
    pthread_mutex_lock(&g_shm_pool->header.mutex);
    
    if (timeout_ms == 0) {
        // 非阻塞检查
        int has_packets = (g_shm_pool->header.packet_count > 0);
        pthread_mutex_unlock(&g_shm_pool->header.mutex);
        return has_packets ? SUCCESS : ERROR_INVALID_PACKET;
    }
    
    // 等待新包
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += (timeout_ms % 1000) * 1000000;
    ts.tv_sec += timeout_ms / 1000 + ts.tv_nsec / 1000000000;
    ts.tv_nsec %= 1000000000;
    
    int ret = pthread_cond_timedwait(&g_shm_pool->header.cond, &g_shm_pool->header.mutex, &ts);
    pthread_mutex_unlock(&g_shm_pool->header.mutex);
    
    return (ret == 0) ? SUCCESS : ERROR_INVALID_PACKET;
}

// 通知新包到达
void notify_new_packet() {
    pthread_cond_broadcast(&g_shm_pool->header.cond);
}

// 清理共享内存
void cleanup_shared_memory() {
    if (g_shm_pool) {
        munmap(g_shm_pool, SHM_SIZE);
        g_shm_pool = NULL;
    }
    
    if (g_shm_fd >= 0) {
        close(g_shm_fd);
        g_shm_fd = -1;
    }
    
    printf("[SHM] 共享内存清理完成\n");
}

// 打印包描述符信息
void print_packet_descriptor(const packet_descriptor_t* desc) {
    if (!desc) {
        printf("[DESC] 无效的包描述符\n");
        return;
    }
    
    printf("[DESC] 包描述符信息:\n");
    printf("  ID: %lu\n", desc->packet_id);
    printf("  数据偏移: %u\n", desc->data_offset);
    printf("  数据大小: %u\n", desc->data_size);
    printf("  函数ID: %u\n", desc->function_id);
    printf("  时间戳: %lu\n", desc->timestamp);
    printf("  标志: 0x%08x\n", desc->flags);
}

// 获取错误字符串
const char* get_error_string(int error_code) {
    switch (error_code) {
        case SUCCESS: return "成功";
        case ERROR_SHM_INIT: return "共享内存初始化失败";
        case ERROR_SHM_ATTACH: return "共享内存附加失败";
        case ERROR_PACKET_FULL: return "包池已满";
        case ERROR_INVALID_PACKET: return "无效的包";
        default: return "未知错误";
    }
}
