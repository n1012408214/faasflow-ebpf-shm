# In template, functions are across different workflows.
import os.path

KAFKA_IP = '192.168.2.157'
GATEWAY_IP = '192.168.2.156'
COUCHDB_IP = '192.168.2.157'
WORKER_ADDRS = ['192.168.2.154', '192.168.2.155']

COUCHDB_URL = f'http://openwhisk:openwhisk@{COUCHDB_IP}:5984/'
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_DB = 0
# RESOURCE_MONITOR_URL = 'http://127.0.0.1:7998/{}'
KAFKA_URL = f'{KAFKA_IP}:9092'
PREFETCHER_URL = 'http://127.0.0.1:8002/{}'
GATEWAY_URL = f'{GATEWAY_IP}:7000'

FUNCTIONS_INFO_PATH = '../../benchmark'
WORKFLOWS_INFO_PATH = {
                       'video': os.path.expanduser('/home/njl/FaaSFlow/benchmark/video'),
                       'wordcount': os.path.expanduser('/home/njl/FaaSFlow/benchmark/wordcount'),
                       'recognizer': os.path.expanduser('/home/njl/FaaSFlow/benchmark/recognizer'),
                       'svd': os.path.expanduser('/home/njl/FaaSFlow/benchmark/svd')}
if os.path.exists('/state/partition2/FaaSFlow'):
    PREFETCH_POOL_PATH = '/state/partition2/FaaSFlow/prefetch_pool'
    FILE_CONTROLLER_PATH = '/state/partition2/FaaSFlow/file_controller'
else:
    PREFETCH_POOL_PATH = os.path.expanduser('/home/njl/FaaSFlow/prefetch_pool')
    FILE_CONTROLLER_PATH = os.path.expanduser('/home/njl/FaaSFlow/file_controller')
CHUNK_SIZE = 1 * 1024 * 1024

DOCKER_CPU_QUOTA = 100000

REDIS_EXPIRE_SECONDS = 100
# COLLECT_CONTAINER_RESOURCE = False
KAFKA_CHUNK_TEST = False
DISABLE_PRESSURE_AWARE = False

# 共享内存配置
ENABLE_SHARED_MEMORY = False  # 是否启用共享内存传输（改为False禁用）
USE_OPTIMIZED_SHM = True  # 是否使用优化的SHM方案（idle队列+哈希表，改为False使用旧的Socket方案）

# 优化SHM配置（USE_OPTIMIZED_SHM=True时生效）
OPTIMIZED_SHM_SIZE = 5 * 1024 * 1024 * 1024  # 优化SHM大小 (5GB)
OPTIMIZED_SHM_NAME = 'faasflow_shm_opt'  # 优化SHM名称
OPTIMIZED_MAX_PACKETS = 5000  # 最大包数量
OPTIMIZED_MAX_PACKET_SIZE = 1 * 1024 * 1024  # 最大包大小 (1MB)

# 旧SHM配置（USE_OPTIMIZED_SHM=False时生效）
SHM_SIZE = 1024 * 1024 * 1024  # 共享内存大小 (1GB)
SHM_NAME = 'faasflow_shm'  # 共享内存名称
SHM_MAGIC = 0x12345678  # 共享内存魔数
SHM_PROTOCOL_VERSION = 1  # 协议版本

# 共享内存传输阈值 (数据大小超过此阈值时使用共享内存)
SHM_THRESHOLD_SIZE = 1024 * 1024  # 1MB

# 共享内存相关端口
SHM_PROXY_PORT = 8081  # 共享内存代理端口
SHM_CONTAINER_PORT = 8082  # 共享内存容器端口

# 性能日志配置
PERF_LOG_DIR = os.path.expanduser('/home/njl/FaaSFlow/log')  # 性能日志目录
PERF_LOG_FILE = os.path.join(PERF_LOG_DIR, 'test.log')  # Worker端性能日志文件
CONTAINER_PERF_LOG_FILE = os.path.join(PERF_LOG_DIR, 'container_perf.log')  # 容器端性能日志文件
CONTAINER_LOG_MOUNT_PATH = '/tmp/faasflow_log'  # 容器内日志挂载路径
