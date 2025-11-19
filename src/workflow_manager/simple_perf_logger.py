import time
import json
import os
from datetime import datetime
from typing import Dict, Optional

class SimplePerfLogger:
    def __init__(self):
        # 优先使用配置文件中的路径
        try:
            import config
            if hasattr(config, 'PERF_LOG_FILE'):
                self.log_file = config.PERF_LOG_FILE
            else:
                self.log_file = os.environ.get('PERF_LOG_FILE', '/home/njl/FaaSFlow/log/test.log')
        except ImportError:
            self.log_file = os.environ.get('PERF_LOG_FILE', '/home/njl/FaaSFlow/log/test.log')
        
        self.request_timers: Dict[str, Dict] = {}
        
        # 确保日志目录存在
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
    
    def start_request(self, request_id: str, workflow_name: str):
        """开始记录请求"""
        self.request_timers[request_id] = {
            'workflow_name': workflow_name,
            'start_time': time.time(),
            'events': []
        }
        self._log_event(request_id, 'REQUEST_START', workflow_name)
    
    def end_request(self, request_id: str):
        """结束请求记录"""
        if request_id in self.request_timers:
            duration = time.time() - self.request_timers[request_id]['start_time']
            self._log_event(request_id, 'REQUEST_END', duration=duration)
            
            # 生成请求摘要
            summary = self._generate_summary(request_id)
            self._write_summary(summary)
            
            del self.request_timers[request_id]
    
    def log_event(self, request_id: str, event_name: str, template_name: str = None, 
                  block_name: str = None, duration: float = None, data_size: int = None):
        """记录事件"""
        if request_id in self.request_timers:
            event = {
                'timestamp': time.time(),
                'event': event_name,
                'template_name': template_name,
                'block_name': block_name,
                'duration': duration,
                'data_size': data_size
            }
            self.request_timers[request_id]['events'].append(event)
            self._log_event(request_id, event_name, template_name, block_name, duration, data_size)
    
    def _log_event(self, request_id: str, event_name: str, workflow_name: str = None,
                   template_name: str = None, block_name: str = None, 
                   duration: float = None, data_size: int = None):
        """写入事件到日志文件"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'request_id': request_id,
            'workflow_name': workflow_name,
            'event': event_name,
            'template_name': template_name,
            'block_name': block_name,
            'duration': duration,
            'data_size': data_size
        }
        
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
        except Exception as e:
            print(f"写入性能日志失败: {e}")
    
    def _generate_summary(self, request_id: str) -> Dict:
        """生成请求摘要"""
        if request_id not in self.request_timers:
            return {}
        
        timer = self.request_timers[request_id]
        events = timer['events']
        
        summary = {
            'request_id': request_id,
            'workflow_name': timer['workflow_name'],
            'total_duration': time.time() - timer['start_time'],
            'total_events': len(events),
            'block_executions': [],
            'data_transfers': [],
            'model_operations': []
        }
        
        for event in events:
            if event['event'] == 'BLOCK_EXECUTION_END':
                summary['block_executions'].append({
                    'block_name': event['block_name'],
                    'duration': event['duration']
                })
            elif event['event'] == 'DATA_TRANSFER_END':
                summary['data_transfers'].append({
                    'data_size': event['data_size'],
                    'duration': event['duration']
                })
            elif event['event'] in ['MODEL_LOAD_END', 'INFERENCE_END']:
                summary['model_operations'].append({
                    'operation': event['event'],
                    'duration': event['duration']
                })
        
        return summary
    
    def _write_summary(self, summary: Dict):
        """写入摘要到单独的文件"""
        if not summary:
            return
        
        summary_file = self.log_file.replace('.log', '_summary.log')
        try:
            with open(summary_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
        except Exception as e:
            print(f"写入摘要失败: {e}")

# 全局日志记录器实例
perf_logger = SimplePerfLogger()

# 便捷函数
def start_request_timer(request_id: str, workflow_name: str):
    """开始请求计时"""
    perf_logger.start_request(request_id, workflow_name)

def end_request_timer(request_id: str):
    """结束请求计时"""
    perf_logger.end_request(request_id)

def log_event(request_id: str, event_name: str, template_name: str = None, 
              block_name: str = None, duration: float = None, data_size: int = None):
    """记录事件"""
    perf_logger.log_event(request_id, event_name, template_name, block_name, duration, data_size)

# 常用事件记录函数
def log_block_execution(request_id: str, template_name: str, block_name: str, duration: float):
    """记录块执行时间"""
    log_event(request_id, 'BLOCK_EXECUTION_END', template_name, block_name, duration)

def log_data_transfer(request_id: str, template_name: str, data_size: int, duration: float):
    """记录数据传输时间"""
    log_event(request_id, 'DATA_TRANSFER_END', template_name, data_size=data_size, duration=duration)

def log_model_load(request_id: str, template_name: str, duration: float):
    """记录模型加载时间"""
    log_event(request_id, 'MODEL_LOAD_END', template_name, duration=duration)

def log_inference(request_id: str, template_name: str, duration: float):
    """记录推理时间"""
    log_event(request_id, 'INFERENCE_END', template_name, duration=duration)

def log_container_start(request_id: str, template_name: str):
    """记录容器启动"""
    log_event(request_id, 'CONTAINER_START', template_name)

def log_container_ready(request_id: str, template_name: str):
    """记录容器就绪"""
    log_event(request_id, 'CONTAINER_READY', template_name)
