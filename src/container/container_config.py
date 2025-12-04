import os

COUCHDB_URL = 'http://openwhisk:openwhisk@192.168.2.157:5984/'
KAFKA_URL = '192.168.2.157:9092'
KAFKA_CHUNK_SIZE = 256 * 1024
KAFKA_NUM_PARTITIONS = 8
KAFKA_NUM_TOPICS = 1
REDIS_HOST = '172.17.0.1'
REDIS_PORT = 6379
REDIS_DB = 0
REMOTE_DB = 'KAFKA'
ENABLE_SHARED_MEMORY = True  # 是否启用共享内存传输
USE_OPTIMIZED_SHM = True  # 是否使用优化的SHM方案（idle队列+哈希表）

OPTIMIZED_SHM_SIZE = 5 * 1024 * 1024 * 1024  # 优化SHM大小 (5GB)
OPTIMIZED_SHM_NAME = 'faasflow_shm_opt'  # 优化SHM名称
OPTIMIZED_MAX_PACKETS = 5000  # 最大包数量
OPTIMIZED_MAX_PACKET_SIZE = 1 * 1024 * 1024  # 最大包大小 (1MB)
QUEUE_METHOD = 'local'  # local 或 shared
LOCAL_QUEUE_HIGH = 64  # 本地队列高水位（触发归还）
LOCAL_QUEUE_LOW = 32    # 本地队列低水位（触发补给）
LOCAL_QUEUE_INIT_BATCH = 48  # 初始化时批量领取的描述符数量
LOCAL_QUEUE_REFILL_BATCH = 16  # 本地枯竭时从全局批量领取数量
LOCAL_QUEUE_RETURN_BATCH = 16  # 本地高水位时批量归还数量
# 旧SHM配置（USE_OPTIMIZED_SHM=False时生效）
SHM_SIZE = 1024 * 1024 * 100  # 共享内存大小 (100MB)
SHM_NAME = 'faasflow_shm'  # 共享内存名称
SHM_MAGIC = 0x12345678  # 共享内存魔数
SHM_PROTOCOL_VERSION = 1  # 协议版本

# 共享内存传输阈值 (数据大小超过此阈值时使用共享内存)
SHM_THRESHOLD_SIZE = 1024 * 1024  # 1MB

# 共享内存相关端口
SHM_PROXY_PORT = 8081  # 共享内存代理端口
SHM_CONTAINER_PORT = 8082  # 共享内存容器端口

SOCKET_CHUNK_SIZE = 1024 * 1024

CONTAINER_PORT = int(os.environ.get('FAASFLOW_CONTAINER_PORT', '0') or 0)
