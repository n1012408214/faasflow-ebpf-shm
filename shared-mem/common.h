#ifndef COMMON_H
#define COMMON_H

#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>

// 共享内存配置
#define SHM_SIZE (5 * 1024 * 1024 * 1024)  // 5GB共享内存
#define MAX_PACKETS 5000                    // 最大包数量
#define MAX_PACKET_SIZE (1 * 1024 * 1024)  // 最大包大小1MB
#define SHM_NAME "/faasflow_shm"       // 共享内存名称
#define SEM_NAME "/faasflow_sem"       // 信号量名称

// 包描述符结构
typedef struct {
    uint64_t packet_id;           // 包唯一ID
    uint32_t data_offset;         // 数据在共享内存中的偏移
    uint32_t data_size;           // 数据大小
    uint32_t function_id;         // 目标函数ID
    uint64_t timestamp;           // 时间戳
    uint32_t flags;               // 标志位
    char reserved[32];            // 保留字段
} packet_descriptor_t;

// 共享内存池头部
typedef struct {
    uint32_t magic;               // 魔数，用于验证
    uint32_t version;             // 版本号
    uint32_t total_size;          // 总大小
    uint32_t data_offset;         // 数据区域偏移
    uint32_t data_size;           // 数据区域大小
    uint32_t packet_count;        // 当前包数量
    uint32_t next_packet_id;      // 下一个包ID
    pthread_mutex_t mutex;        // 互斥锁
    pthread_cond_t cond;          // 条件变量
} shm_header_t;

// 共享内存池完整结构
typedef struct {
    shm_header_t header;          // 头部信息
    packet_descriptor_t descriptors[MAX_PACKETS];  // 包描述符数组
    char data_area[];             // 数据区域（柔性数组）
} shared_memory_pool_t;

// 魔数和版本
#define SHM_MAGIC 0x12345678
#define SHM_VERSION 1

// 包标志位
#define PACKET_FLAG_VALID     0x00000001
#define PACKET_FLAG_PROCESSED 0x00000002
#define PACKET_FLAG_ERROR     0x00000004

// 函数返回码
#define SUCCESS 0
#define ERROR_SHM_INIT -1
#define ERROR_SHM_ATTACH -2
#define ERROR_PACKET_FULL -3
#define ERROR_INVALID_PACKET -4

// 工具函数声明
uint64_t get_timestamp();
void print_packet_descriptor(const packet_descriptor_t* desc);
const char* get_error_string(int error_code);

#endif // COMMON_H
