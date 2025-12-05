import logging
import logging.handlers
import queue
import json
import os
import sys
import signal
import threading
import uuid
import random
import time
import platform
import re
from datetime import datetime
from typing import Dict, Optional, Any, Union, Callable
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from .config_loader import ConfigLoader

__all__ = [
    "get_logger", 
    "update_log_level", 
    "enrich_log_record", 
    "shutdown_logger", 
    "correlation_decorator",
    "CorrelationMiddleware",
    "inject_custom_fields",
    "get_correlation_id",
    "log_metric"
]

class PIISanitizer:
    PATTERNS = {
        'credit_card': r'\b(?:\d[ -]*?){13,16}\b',
        'phone': r'\b(?:\+?\d{1,3})?[-.\s]?\d{10}\b',
        'ssn': r'\b\d{3}[-]?\d{2}[-]?\d{4}\b',
        'account_number': r'\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b'
    }
    
    REPLACEMENTS = {
        'credit_card': lambda m: m.group()[:4] + ' **** **** ' + m.group()[-4:],
        'phone': lambda m: '***-***-' + m.group()[-4:],
        'ssn': lambda m: '***-**-' + m.group()[-4:],
        'account_number': lambda m: m.group()[:4] + '*' * (len(m.group()) - 8) + m.group()[-4:]
    }
    
    @classmethod
    def sanitize(cls, text: str) -> str:
        for pattern_name, pattern in cls.PATTERNS.items():
            if pattern_name in cls.REPLACEMENTS:
                replacement = cls.REPLACEMENTS[pattern_name]
                text = re.sub(pattern, replacement, text)
        return text

class JsonFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.hostname = platform.node()

    def format(self, record: logging.LogRecord) -> str:
        correlation_id = getattr(record, 'correlation_id', None) or str(uuid.uuid4())[:8]
        record.correlation_id = correlation_id
        
        message = record.getMessage()
        sanitized_message = PIISanitizer.sanitize(message)
        
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + f".{int(datetime.utcnow().microsecond/1000):03d}",
            "level": record.levelname,
            "module": record.name,
            "message": sanitized_message,
            "correlation_id": correlation_id,
            "thread_id": threading.get_ident(),
            "process_id": os.getpid(),
            "host": self.hostname
        }
        
        if hasattr(record, 'custom_fields'):
            log_data.update(record.custom_fields)
        
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, ensure_ascii=False)

class EnhancedColorFormatter(logging.Formatter):
    COLOR_MAP = {
        logging.DEBUG: "\033[36m",
        logging.INFO: "\033[32m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[1;31m"
    }
    RESET = "\033[0m"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(
            '[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(name)s] [%(correlation_id)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.use_color = config.get("logging", {}).get("use_color", True)

    def format(self, record: logging.LogRecord) -> str:
        correlation_id = getattr(record, 'correlation_id', None) or str(uuid.uuid4())[:8]
        record.correlation_id = correlation_id
        
        record.message = PIISanitizer.sanitize(record.getMessage())
        formatted = super().format(record)
        if self.use_color and record.levelno in self.COLOR_MAP:
            return f"{self.COLOR_MAP[record.levelno]}{formatted}{self.RESET}"
        return formatted

class EnterpriseFileFormatter(logging.Formatter):
    def __init__(self, use_json: bool = False, max_message_length: int = 10000):
        super().__init__()
        self.use_json = use_json
        self.max_message_length = max_message_length
        self.hostname = platform.node()
        if not use_json:
            self._style._fmt = '[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(name)s] [%(correlation_id)s] %(message)s'
            self.datefmt = '%Y-%m-%d %H:%M:%S'

    def format(self, record: logging.LogRecord) -> str:
        correlation_id = getattr(record, 'correlation_id', None) or str(uuid.uuid4())[:8]
        record.correlation_id = correlation_id
        
        original_message = record.getMessage()
        sanitized_message = PIISanitizer.sanitize(original_message)
        
        if len(sanitized_message) > self.max_message_length:
            record.message = sanitized_message[:self.max_message_length] + "...[truncated]"
        else:
            record.message = sanitized_message
        
        if self.use_json:
            log_data = {
                "timestamp": datetime.utcnow().isoformat() + f".{int(datetime.utcnow().microsecond/1000):03d}",
                "level": record.levelname,
                "module": record.name,
                "message": record.message,
                "correlation_id": correlation_id,
                "thread_id": threading.get_ident(),
                "process_id": os.getpid(),
                "host": self.hostname
            }
            
            if hasattr(record, 'custom_fields'):
                log_data.update(record.custom_fields)
            
            if record.exc_info:
                log_data["exception"] = self.formatException(record.exc_info)
            return json.dumps(log_data, ensure_ascii=False)
        
        return super().format(record)

class RateLimiter:
    def __init__(self, max_messages: int = 10, window_seconds: int = 5):
        self.max_messages = max_messages
        self.window_seconds = window_seconds
        self.message_history = {}
        self.lock = threading.Lock()
    
    def should_log(self, message: str, level: str) -> bool:
        with self.lock:
            current_time = time.time()
            key = f"{level}:{hash(message)}"
            
            if key not in self.message_history:
                self.message_history[key] = []
            
            self.message_history[key] = [
                ts for ts in self.message_history[key]
                if current_time - ts < self.window_seconds
            ]
            
            if len(self.message_history[key]) >= self.max_messages:
                return False
            
            self.message_history[key].append(current_time)
            return True

class ConfigWatcher(threading.Thread):
    def __init__(self, config_path: str, callback: Callable, interval: int = 30):
        super().__init__(daemon=True)
        self.config_path = config_path
        self.callback = callback
        self.interval = interval
        self.running = True
        try:
            self.last_mtime = os.path.getmtime(self.config_path)
        except Exception:
            self.last_mtime = 0
    
    def run(self):
        while self.running:
            try:
                current_mtime = os.path.getmtime(self.config_path)
                if current_mtime != self.last_mtime:
                    self.last_mtime = current_mtime
                    self.callback()
            except Exception:
                pass
            time.sleep(self.interval)
    
    def stop(self):
        self.running = False

class AsyncErrorReporter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ErrorReporter")
        self._observability_hooks = []
    
    def register_hook(self, hook: Callable):
        self._observability_hooks.append(hook)
    
    def report_error(self, record: logging.LogRecord):
        if record.levelno >= logging.ERROR:
            for hook in self._observability_hooks:
                try:
                    self.executor.submit(hook, record)
                except Exception:
                    pass

class SafeRotator:
    @staticmethod
    def rotator(source, dest):
        try:
            timestamp = int(time.time() * 1000)
            base_name = os.path.basename(source)
            dir_name = os.path.dirname(source)
            new_dest = os.path.join(dir_name, f"{base_name}.{timestamp}")
            os.rename(source, new_dest)
        except Exception:
            os.rename(source, dest)
    
    @staticmethod
    def namer(name):
        return name

class Logger:
    _instance_lock = threading.Lock()
    _instance: Optional['Logger'] = None
    _loggers: Dict[str, logging.Logger] = {}
    _config_loaded = False
    _correlation_store = threading.local()
    _module_routes = {}
    _logger_creation_lock = threading.Lock()

    def __new__(cls):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._config_lock = threading.Lock()
            self._config = {}
            self._log_queue = queue.Queue(maxsize=10000)
            self._queue_handler = None
            self._file_handler = None
            self._error_handler = None
            self._metrics_handler = None
            self._console_handler = None
            self._listener = None
            self._sampling_rates = {}
            self._config_watcher = None
            self._rate_limiter = RateLimiter()
            self._error_reporter = None
            self._module_handlers = {}
            self._config_path = "config/settings.json"
            self._reload_config()
            self._setup_logging_infrastructure()
            self._setup_global_exception_handler()
            self._setup_signal_handlers()
            self._start_config_watcher()
            self._setup_external_observability()
            self._initialized = True

    def _reload_config(self):
        with self._config_lock:
            try:
                config = ConfigLoader.load_config(self._config_path)
                if not config:
                    config = {}
                self._config = config
                self._config_loaded = True
            except Exception:
                self._config = {
                    "logging": {
                        "log_dir": "logs/",
                        "log_file": "app.log",
                        "max_bytes": 5 * 1024 * 1024,
                        "backup_count": 10,
                        "use_color": True,
                        "use_json": False,
                        "level": "INFO",
                        "sampling_rates": {
                            "DEBUG": 0.1,
                            "INFO": 1.0,
                            "WARNING": 1.0,
                            "ERROR": 1.0,
                            "CRITICAL": 1.0
                        },
                        "max_message_length": 10000,
                        "config_watch_interval": 30,
                        "rate_limit_messages": 10,
                        "rate_limit_window": 5,
                        "export_to_elk": False,
                        "export_to_prometheus": False,
                        "enable_pii_sanitization": True,
                        "enable_async_error_reporting": True
                    }
                }
                self._config_loaded = False

    def _setup_external_observability(self):
        log_config = self._config.get("logging", {})
        
        self._error_reporter = AsyncErrorReporter(self._config)
        
        if log_config.get("export_to_elk", False):
            def elk_hook(record):
                pass
            self._error_reporter.register_hook(elk_hook)
        
        if log_config.get("export_to_prometheus", False):
            def prometheus_hook(record):
                pass
            self._error_reporter.register_hook(prometheus_hook)

    def _start_config_watcher(self):
        try:
            watch_interval = self._config.get("logging", {}).get("config_watch_interval", 30)
            if watch_interval > 0:
                self._config_watcher = ConfigWatcher(
                    config_path=self._config_path,
                    callback=self._on_config_change,
                    interval=watch_interval
                )
                self._config_watcher.start()
        except Exception:
            pass

    def _on_config_change(self):
        old_config = self._config.copy()
        with self._config_lock:
            self._reload_config()
        
        old_log_config = old_config.get("logging", {})
        new_log_config = self._config.get("logging", {})
        
        changed = False
        for key in ["use_json", "use_color", "max_bytes", "backup_count", "max_message_length"]:
            if old_log_config.get(key) != new_log_config.get(key):
                changed = True
                break
        
        if changed:
            self._recreate_logging_infrastructure()
        
        if old_log_config.get("level") != new_log_config.get("level"):
            level_name = new_log_config.get("level", "INFO").upper()
            if level_name in logging._nameToLevel:
                self.update_log_level(level_name)
        
        logger = self.get_logger("LoggerManager")
        logger.info("Configuration reloaded from settings.json")

    def _recreate_logging_infrastructure(self):
        self.shutdown_infrastructure()
        self._setup_logging_infrastructure()

    def _get_log_config(self) -> Dict[str, Any]:
        with self._config_lock:
            return self._config.get("logging", {})

    def _setup_logging_infrastructure(self):
        log_config = self._get_log_config()
        log_dir = log_config.get("log_dir", "logs/")
        
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, log_config.get("log_file", "app.log"))
        max_bytes = log_config.get("max_bytes", 5 * 1024 * 1024)
        backup_count = log_config.get("backup_count", 10)
        use_json = log_config.get("use_json", False)
        max_message_length = log_config.get("max_message_length", 10000)
        
        self._sampling_rates = log_config.get("sampling_rates", {
            "DEBUG": 0.1,
            "INFO": 1.0,
            "WARNING": 1.0,
            "ERROR": 1.0,
            "CRITICAL": 1.0
        })

        self._rate_limiter = RateLimiter(
            max_messages=log_config.get("rate_limit_messages", 10),
            window_seconds=log_config.get("rate_limit_window", 5)
        )

        self._queue_handler = logging.handlers.QueueHandler(self._log_queue)
        
        level_name = log_config.get("level", "INFO").upper()
        if level_name not in logging._nameToLevel:
            level_name = "INFO"
        log_level = logging._nameToLevel[level_name]
        self._queue_handler.setLevel(log_level)
        
        file_formatter = EnterpriseFileFormatter(use_json=use_json, max_message_length=max_message_length)
        
        self._file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        self._file_handler.setFormatter(file_formatter)
        self._file_handler.rotator = SafeRotator.rotator
        self._file_handler.namer = SafeRotator.namer

        error_log_file = os.path.join(log_dir, "error.log")
        self._error_handler = logging.handlers.RotatingFileHandler(
            filename=error_log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        error_formatter = EnterpriseFileFormatter(use_json=False)
        self._error_handler.setFormatter(error_formatter)
        self._error_handler.setLevel(logging.ERROR)
        self._error_handler.rotator = SafeRotator.rotator
        self._error_handler.namer = SafeRotator.namer

        metrics_log_file = os.path.join(log_dir, "metrics.log")
        self._metrics_handler = logging.handlers.RotatingFileHandler(
            filename=metrics_log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        metrics_formatter = EnterpriseFileFormatter(use_json=True)
        self._metrics_handler.setFormatter(metrics_formatter)
        self._metrics_handler.setLevel(logging.INFO)
        self._metrics_handler.rotator = SafeRotator.rotator
        self._metrics_handler.namer = SafeRotator.namer

        console_formatter = EnhancedColorFormatter(self._config)
        self._console_handler = logging.StreamHandler(sys.stdout)
        self._console_handler.setFormatter(console_formatter)

        self._listener = logging.handlers.QueueListener(
            self._log_queue,
            self._file_handler,
            self._error_handler,
            self._metrics_handler,
            self._console_handler,
            respect_handler_level=True
        )
        self._listener.start()
        
        try:
            if hasattr(self._listener, '_thread'):
                self._listener._thread.daemon = True
        except Exception:
            pass

    def _setup_global_exception_handler(self):
        def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
            emergency_logger = logging.getLogger("EmergencyGlobalLogger")
            emergency_logger.propagate = False
            if not emergency_logger.handlers:
                emergency_handler = logging.StreamHandler(sys.stderr)
                emergency_handler.setFormatter(EnhancedColorFormatter(self._config))
                emergency_logger.addHandler(emergency_handler)
                emergency_logger.addHandler(self._queue_handler)
            
            emergency_logger.critical(
                "Unhandled system exception",
                exc_info=(exc_type, exc_value, exc_traceback)
            )
            
            if self._error_reporter:
                record = logging.LogRecord(
                    "EmergencyGlobalLogger",
                    logging.CRITICAL,
                    "", 0,
                    "Unhandled system exception",
                    None,
                    (exc_type, exc_value, exc_traceback)
                )
                self._error_reporter.report_error(record)

        sys.excepthook = handle_uncaught_exception
        threading.excepthook = lambda args: handle_uncaught_exception(
            args.exc_type, args.exc_value, args.exc_traceback
        )

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            shutdown_logger()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def get_logger(self, module_name: str) -> logging.Logger:
        with self._logger_creation_lock:
            if module_name in self._loggers:
                return self._loggers[module_name]

            logger = logging.getLogger(module_name)
            
            log_config = self._get_log_config()
            level_name = log_config.get("level", "INFO").upper()
            if level_name not in logging._nameToLevel:
                level_name = "INFO"
            log_level = logging._nameToLevel[level_name]
            logger.setLevel(log_level)
            
            logger.propagate = False
            
            if not logger.handlers:
                handler = self._create_enhanced_handler()
                logger.addHandler(handler)

            self._loggers[module_name] = logger
            return logger

    def _create_enhanced_handler(self):
        class EnhancedHandler(logging.handlers.QueueHandler):
            def __init__(self, queue, rate_limiter, error_reporter, sampling_rates):
                super().__init__(queue)
                self.rate_limiter = rate_limiter
                self.error_reporter = error_reporter
                self.sampling_rates = sampling_rates
            
            def emit(self, record):
                sampling_rate = self.sampling_rates.get(record.levelname, 1.0)
                if sampling_rate < 1.0 and random.random() > sampling_rate:
                    return
                
                if not self.rate_limiter.should_log(record.getMessage(), record.levelname):
                    return
                
                correlation_id = getattr(record, 'correlation_id', None) or Logger().get_correlation_id() or str(uuid.uuid4())[:8]
                record.correlation_id = correlation_id
                
                if self.error_reporter and record.levelno >= logging.ERROR:
                    self.error_reporter.report_error(record)
                
                super().emit(record)
        
        return EnhancedHandler(self._log_queue, self._rate_limiter, self._error_reporter, self._sampling_rates)

    def update_log_level(self, new_level: str) -> None:
        new_level = new_level.upper()
        if new_level not in logging._nameToLevel:
            new_level = "INFO"
        
        effective_level = logging._nameToLevel[new_level]
        
        with self._logger_creation_lock:
            for logger in self._loggers.values():
                logger.setLevel(effective_level)

    def set_correlation_id(self, correlation_id: str):
        self._correlation_store.correlation_id = correlation_id

    def get_correlation_id(self) -> Optional[str]:
        return getattr(self._correlation_store, 'correlation_id', None)

    def log_metric(self, name: str, value: Any, tags: Optional[Dict[str, Any]] = None):
        metrics_logger = logging.getLogger("METRICS")
        
        if not metrics_logger.handlers:
            metrics_logger.addHandler(self._queue_handler)
            metrics_logger.setLevel(logging.INFO)
            metrics_logger.propagate = False
        
        metric_data = {
            "metric_name": name,
            "value": value,
            "tags": tags or {},
            "timestamp": datetime.utcnow().isoformat() + f".{int(datetime.utcnow().microsecond/1000):03d}"
        }
        
        old_factory = metrics_logger.makeRecord
        
        def makeRecord_with_metric(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            if not hasattr(record, 'custom_fields'):
                record.custom_fields = {}
            record.custom_fields.update(metric_data)
            return record
        
        metrics_logger.makeRecord = makeRecord_with_metric
        try:
            metrics_logger.info(json.dumps(metric_data))
        finally:
            metrics_logger.makeRecord = old_factory

    def enrich_log_record(self, correlation_id: Optional[str] = None) -> Dict[str, Any]:
        hostname = platform.node()
        cid = correlation_id or self.get_correlation_id() or str(uuid.uuid4())
        return {
            "correlation_id": cid,
            "timestamp": datetime.utcnow().isoformat() + f".{int(datetime.utcnow().microsecond/1000):03d}",
            "thread_id": threading.get_ident(),
            "process_id": os.getpid(),
            "host": hostname
        }

    def shutdown_infrastructure(self):
        if self._listener:
            try:
                self._listener.stop()
            except Exception:
                pass
            self._listener = None
        
        if self._file_handler:
            self._file_handler.close()
            self._file_handler = None
        
        if self._error_handler:
            self._error_handler.close()
            self._error_handler = None
        
        if self._metrics_handler:
            self._metrics_handler.close()
            self._metrics_handler = None
        
        if self._console_handler:
            self._console_handler.close()
            self._console_handler = None

    def shutdown(self):
        if self._config_watcher:
            self._config_watcher.stop()
            self._config_watcher = None
        
        self.shutdown_infrastructure()
        
        if self._error_reporter:
            self._error_reporter.executor.shutdown(wait=False)
            self._error_reporter = None

_logger_manager_instance: Optional[Logger] = None
_logger_manager_lock = threading.Lock()

def get_logger(module_name: str) -> logging.Logger:
    global _logger_manager_instance
    with _logger_manager_lock:
        if _logger_manager_instance is None:
            _logger_manager_instance = Logger()
        return _logger_manager_instance.get_logger(module_name)

def update_log_level(new_level: str) -> None:
    with _logger_manager_lock:
        if _logger_manager_instance:
            _logger_manager_instance.update_log_level(new_level)

def set_correlation_id(correlation_id: str):
    with _logger_manager_lock:
        if _logger_manager_instance:
            _logger_manager_instance.set_correlation_id(correlation_id)

def get_correlation_id() -> Optional[str]:
    with _logger_manager_lock:
        if _logger_manager_instance:
            return _logger_manager_instance.get_correlation_id()
    return None

def enrich_log_record(correlation_id: Optional[str] = None) -> Dict[str, Any]:
    with _logger_manager_lock:
        if _logger_manager_instance:
            return _logger_manager_instance.enrich_log_record(correlation_id)
        hostname = platform.node()
        return {
            "correlation_id": correlation_id or str(uuid.uuid4()),
            "host": hostname
        }

def log_metric(name: str, value: Any, tags: Optional[Dict[str, Any]] = None):
    with _logger_manager_lock:
        if _logger_manager_instance:
            _logger_manager_instance.log_metric(name, value, tags)

def inject_custom_fields(custom_fields: Dict[str, Any]):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            old_factory = logger.makeRecord
            
            def makeRecord_with_fields(*args, **kwargs):
                record = old_factory(*args, **kwargs)
                if not hasattr(record, 'custom_fields'):
                    record.custom_fields = {}
                record.custom_fields.update(custom_fields)
                return record
            
            logger.makeRecord = makeRecord_with_fields
            try:
                return func(*args, **kwargs)
            finally:
                logger.makeRecord = old_factory
        return wrapper
    return decorator

def shutdown_logger():
    global _logger_manager_instance
    with _logger_manager_lock:
        if _logger_manager_instance:
            _logger_manager_instance.shutdown()
            _logger_manager_instance = None

class CorrelationMiddleware:
    def __init__(self, get_response=None):
        self.get_response = get_response
    
    def __call__(self, request):
        correlation_id = request.headers.get('X-Correlation-ID') or str(uuid.uuid4())
        set_correlation_id(correlation_id)
        
        start_time = time.time()
        response = self.get_response(request) if self.get_response else None
        
        if response and hasattr(response, 'headers'):
            response.headers['X-Correlation-ID'] = correlation_id
        
        duration = (time.time() - start_time) * 1000
        logger = get_logger(self.__class__.__module__)
        logger.info(f"Request completed in {duration:.2f}ms")
        
        return response
    
    def process_view(self, request, view_func, view_args, view_kwargs):
        logger = get_logger(view_func.__module__)
        logger.info(f"Processing {view_func.__name__}")

def correlation_decorator(correlation_id: Optional[str] = None):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cid = correlation_id or get_correlation_id() or str(uuid.uuid4())[:8]
            set_correlation_id(cid)
            
            logger = get_logger(func.__module__)
            old_factory = logger.makeRecord
            
            def makeRecord_with_correlation(*args, **kwargs):
                record = old_factory(*args, **kwargs)
                record.correlation_id = cid
                if hasattr(record, 'custom_fields'):
                    record.custom_fields['correlation_id'] = cid
                return record
            
            logger.makeRecord = makeRecord_with_correlation
            start_time = time.time()
            try:
                logger.info(f"Starting {func.__name__}", extra={'correlation_id': cid})
                result = func(*args, **kwargs)
                duration = (time.time() - start_time) * 1000
                logger.info(f"Completed {func.__name__} in {duration:.2f}ms", extra={'correlation_id': cid})
                return result
            except Exception as e:
                duration = (time.time() - start_time) * 1000
                logger.error(f"Failed {func.__name__} after {duration:.2f}ms: {str(e)}", extra={'correlation_id': cid})
                raise
            finally:
                logger.makeRecord = old_factory
        return wrapper
    return decorator