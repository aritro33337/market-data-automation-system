import threading
import time
import traceback
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable, Any
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from croniter import croniter
import logging

from src.utils.logger import get_logger, log_metric
from src.utils.config_loader import ConfigLoader


class JobType(Enum):
    INTERVAL = "interval"
    CRON = "cron"
    HYBRID = "hybrid"


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class JobExecutionMetrics:
    job_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: JobStatus = JobStatus.PENDING
    duration_seconds: float = 0.0
    retry_count: int = 0
    error_message: Optional[str] = None
    execution_id: str = field(default_factory=lambda: str(time.time()))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_name": self.job_name,
            "execution_id": self.execution_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status.value,
            "duration_seconds": self.duration_seconds,
            "retry_count": self.retry_count,
            "error_message": self.error_message
        }


@dataclass
class JobConfig:
    name: str
    func: Callable
    job_type: JobType
    enabled: bool = True
    interval_seconds: Optional[int] = None
    cron_expression: Optional[str] = None
    max_retries: int = 3
    retry_delay_seconds: float = 2.0
    timeout_seconds: Optional[float] = None
    priority: int = 5
    last_run_time: Optional[datetime] = None
    next_run_time: Optional[datetime] = None


class JobRegistry:
    def __init__(self):
        self.logger = get_logger("JobRegistry")
        self._jobs: Dict[str, JobConfig] = {}
        self._lock = threading.RLock()
        self._execution_history: Dict[str, List[JobExecutionMetrics]] = {}

    def register(self, job_config: JobConfig) -> None:
        try:
            with self._lock:
                if job_config.name in self._jobs:
                    self.logger.warning(f"Job '{job_config.name}' already exists, overwriting")
                
                self._validate_job_config(job_config)
                self._jobs[job_config.name] = job_config
                self._execution_history[job_config.name] = []
                
                self.logger.info(f"Job registered: {job_config.name} (type={job_config.job_type.value})")
                log_metric("job_registered", 1, {"job_name": job_config.name})
        except Exception as e:
            self.logger.error(f"Failed to register job '{job_config.name}': {str(e)}")
            log_metric("job_registered", 0, {"job_name": job_config.name, "error": str(e)})
            raise

    def unregister(self, job_name: str) -> None:
        try:
            with self._lock:
                if job_name in self._jobs:
                    del self._jobs[job_name]
                    self.logger.info(f"Job unregistered: {job_name}")
        except Exception as e:
            self.logger.error(f"Failed to unregister job '{job_name}': {str(e)}")

    def get_job(self, job_name: str) -> Optional[JobConfig]:
        with self._lock:
            return self._jobs.get(job_name)

    def get_all_jobs(self) -> List[JobConfig]:
        with self._lock:
            return list(self._jobs.values())

    def enable_job(self, job_name: str) -> None:
        with self._lock:
            if job_name in self._jobs:
                self._jobs[job_name].enabled = True
                self.logger.info(f"Job enabled: {job_name}")

    def disable_job(self, job_name: str) -> None:
        with self._lock:
            if job_name in self._jobs:
                self._jobs[job_name].enabled = False
                self.logger.info(f"Job disabled: {job_name}")

    def update_last_run(self, job_name: str, run_time: datetime) -> None:
        with self._lock:
            if job_name in self._jobs:
                self._jobs[job_name].last_run_time = run_time

    def add_execution_record(self, metrics: JobExecutionMetrics) -> None:
        try:
            with self._lock:
                if metrics.job_name in self._execution_history:
                    self._execution_history[metrics.job_name].append(metrics)
                    if len(self._execution_history[metrics.job_name]) > 1000:
                        self._execution_history[metrics.job_name] = \
                            self._execution_history[metrics.job_name][-1000:]
        except Exception as e:
            self.logger.error(f"Failed to add execution record: {str(e)}")

    def get_execution_history(self, job_name: str, limit: int = 100) -> List[JobExecutionMetrics]:
        with self._lock:
            history = self._execution_history.get(job_name, [])
            return history[-limit:] if history else []

    def _validate_job_config(self, config: JobConfig) -> None:
        if not config.name or not isinstance(config.name, str):
            raise ValueError("Job name must be a non-empty string")
        
        if config.func is None or not callable(config.func):
            raise ValueError(f"Job '{config.name}' function must be callable")
        
        if config.job_type == JobType.INTERVAL:
            if config.interval_seconds is None or config.interval_seconds <= 0:
                raise ValueError(f"Job '{config.name}' interval must be > 0")
        
        if config.job_type == JobType.HYBRID:
            if (config.interval_seconds is None or config.interval_seconds <= 0) and not config.cron_expression:
                raise ValueError(f"Job '{config.name}' must have interval_seconds or cron_expression")
        
        if config.job_type == JobType.CRON or config.job_type == JobType.HYBRID:
            if config.cron_expression:
                try:
                    croniter(config.cron_expression)
                except Exception as e:
                    raise ValueError(f"Job '{config.name}' invalid cron: {str(e)}")
        
        if config.max_retries < 0:
            raise ValueError(f"Job '{config.name}' max_retries must be >= 0")
        
        if config.retry_delay_seconds < 0:
            raise ValueError(f"Job '{config.name}' retry_delay must be >= 0")


class SchedulingLogic:
    def __init__(self):
        self.logger = get_logger("SchedulingLogic")

    def should_run_interval(self, job: JobConfig, current_time: datetime) -> bool:
        try:
            if job.interval_seconds is None:
                return False
            
            if job.last_run_time is None:
                return True
            
            elapsed = (current_time - job.last_run_time).total_seconds()
            return elapsed >= job.interval_seconds
        except Exception as e:
            self.logger.error(f"Error checking interval for job '{job.name}': {str(e)}")
            return False

    def should_run_cron(self, job: JobConfig, current_time: datetime) -> bool:
        try:
            if not job.cron_expression:
                return False
            
            cron = croniter(job.cron_expression, current_time)
            last_occurrence = cron.get_prev(datetime)
            
            if job.last_run_time is None:
                return True
            
            return last_occurrence > job.last_run_time
        except Exception as e:
            self.logger.error(f"Error checking cron for job '{job.name}': {str(e)}")
            return False

    def should_run_hybrid(self, job: JobConfig, current_time: datetime) -> bool:
        interval_ready = self.should_run_interval(job, current_time)
        cron_ready = self.should_run_cron(job, current_time)
        return interval_ready or cron_ready

    def should_run(self, job: JobConfig, current_time: datetime) -> bool:
        if not job.enabled:
            return False
        
        if job.job_type == JobType.INTERVAL:
            return self.should_run_interval(job, current_time)
        elif job.job_type == JobType.CRON:
            return self.should_run_cron(job, current_time)
        elif job.job_type == JobType.HYBRID:
            return self.should_run_hybrid(job, current_time)
        
        return False


class JobRunner:
    def __init__(self, max_workers: int = 5):
        self.logger = get_logger("JobRunner")
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="job_")
        self._running_jobs: Dict[str, Future] = {}
        self._lock = threading.RLock()

    def run_job(self, job: JobConfig) -> Future:
        try:
            with self._lock:
                if job.name in self._running_jobs:
                    existing_future = self._running_jobs[job.name]
                    if not existing_future.done():
                        self.logger.warning(f"Job '{job.name}' already running, skipping")
                        return existing_future
            
            future = self._executor.submit(self._execute_with_retry, job)
            
            with self._lock:
                self._running_jobs[job.name] = future
            
            return future
        except Exception as e:
            self.logger.error(f"Failed to submit job '{job.name}': {str(e)}")
            raise

    def _execute_with_timeout(self, job: JobConfig) -> None:
        result = [None]
        exc_info = [None]
        
        def target():
            try:
                result[0] = job.func()
            except Exception as e:
                exc_info[0] = e
        
        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=job.timeout_seconds)
        
        if thread.is_alive():
            raise TimeoutError(f"Job '{job.name}' exceeded timeout of {job.timeout_seconds}s")
        
        if exc_info[0]:
            raise exc_info[0]
        
        return result[0]

    def _execute_with_retry(self, job: JobConfig) -> JobExecutionMetrics:
        metrics = JobExecutionMetrics(
            job_name=job.name,
            start_time=datetime.now()
        )
        
        last_exception = None
        
        for attempt in range(job.max_retries + 1):
            try:
                metrics.status = JobStatus.RUNNING
                metrics.retry_count = attempt
                
                self.logger.info(f"Executing job '{job.name}' (attempt {attempt + 1}/{job.max_retries + 1})")
                
                if job.timeout_seconds:
                    self._execute_with_timeout(job)
                else:
                    job.func()
                
                metrics.status = JobStatus.COMPLETED
                metrics.end_time = datetime.now()
                metrics.duration_seconds = (metrics.end_time - metrics.start_time).total_seconds()
                
                self.logger.info(f"Job '{job.name}' completed in {metrics.duration_seconds:.2f}s")
                log_metric("job_completed", 1, {
                    "job_name": job.name,
                    "duration": metrics.duration_seconds,
                    "attempt": attempt + 1
                })
                
                return metrics
            
            except Exception as e:
                last_exception = e
                metrics.status = JobStatus.FAILED
                metrics.error_message = str(e)
                
                self.logger.error(f"Job '{job.name}' failed (attempt {attempt + 1}): {str(e)}")
                
                if attempt < job.max_retries:
                    self.logger.info(f"Retrying job '{job.name}' in {job.retry_delay_seconds}s")
                    time.sleep(job.retry_delay_seconds)
                else:
                    metrics.end_time = datetime.now()
                    metrics.duration_seconds = (metrics.end_time - metrics.start_time).total_seconds()
                    
                    error_details = {
                        "job_name": job.name,
                        "error": str(e),
                        "duration": metrics.duration_seconds,
                        "attempts": job.max_retries + 1
                    }
                    
                    log_metric("job_failed", 0, error_details)
                    
                    self.logger.error(f"Job '{job.name}' exhausted all retries. Error: {str(e)}\n{traceback.format_exc()}")
        
        metrics.end_time = datetime.now()
        metrics.duration_seconds = (metrics.end_time - metrics.start_time).total_seconds()
        return metrics

    def is_job_running(self, job_name: str) -> bool:
        with self._lock:
            if job_name in self._running_jobs:
                return not self._running_jobs[job_name].done()
            return False

    def wait_for_job(self, job_name: str, timeout: Optional[float] = None) -> Optional[JobExecutionMetrics]:
        try:
            with self._lock:
                future = self._running_jobs.get(job_name)
            
            if future is None:
                return None
            
            return future.result(timeout=timeout)
        except Exception as e:
            self.logger.error(f"Error waiting for job '{job_name}': {str(e)}")
            return None

    def shutdown(self, wait: bool = True, timeout: float = 30.0) -> None:
        try:
            self.logger.info("Shutting down JobRunner")
            
            if wait:
                with self._lock:
                    running_jobs = {k: v for k, v in self._running_jobs.items() if not v.done()}
                
                if running_jobs:
                    self.logger.info(f"Waiting for {len(running_jobs)} running jobs to complete (timeout={timeout}s)")
                    
                    start_time = time.time()
                    while running_jobs and (time.time() - start_time) < timeout:
                        time.sleep(0.5)
                        with self._lock:
                            running_jobs = {k: v for k, v in self._running_jobs.items() if not v.done()}
                    
                    if running_jobs:
                        self.logger.warning(f"{len(running_jobs)} jobs still running after timeout")
            
            self._executor.shutdown(wait=wait)
            self.logger.info("JobRunner shutdown complete")
        except Exception as e:
            self.logger.error(f"Error during JobRunner shutdown: {str(e)}")


class SchedulerEngine:
    def __init__(self, registry: JobRegistry, runner: JobRunner, scheduling_logic: SchedulingLogic):
        self.logger = get_logger("SchedulerEngine")
        self.registry = registry
        self.runner = runner
        self.scheduling_logic = scheduling_logic
        
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._check_interval = 0.3

    def start(self) -> None:
        try:
            with self._lock:
                if self._running:
                    self.logger.warning("Scheduler already running")
                    return
                
                self._running = True
                self._stop_event.clear()
                self._scheduler_thread = threading.Thread(
                    target=self._scheduler_loop,
                    name="SchedulerEngine",
                    daemon=False
                )
                self._scheduler_thread.start()
                
                self.logger.info("Scheduler engine started")
                log_metric("scheduler_started", 1, {})
        except Exception as e:
            self.logger.error(f"Failed to start scheduler: {str(e)}")
            log_metric("scheduler_started", 0, {"error": str(e)})
            raise

    def stop(self, timeout: float = 30.0) -> None:
        try:
            with self._lock:
                if not self._running:
                    self.logger.debug("Scheduler not running")
                    return
                
                self.logger.info("Stopping scheduler engine")
                self._stop_event.set()
            
            if self._scheduler_thread:
                self._scheduler_thread.join(timeout=timeout)
                if self._scheduler_thread.is_alive():
                    self.logger.warning("Scheduler thread did not terminate within timeout")
            
            with self._lock:
                self._running = False
            
            self.logger.info("Scheduler engine stopped")
            log_metric("scheduler_stopped", 1, {})
        except Exception as e:
            self.logger.error(f"Error stopping scheduler: {str(e)}")

    def _scheduler_loop(self) -> None:
        try:
            self.logger.info("Scheduler loop started")
            
            while not self._stop_event.is_set():
                try:
                    current_time = datetime.now()
                    jobs = self.registry.get_all_jobs()
                    
                    for job in jobs:
                        try:
                            if self.scheduling_logic.should_run(job, current_time):
                                self.logger.debug(f"Scheduling job: {job.name}")
                                
                                future = self.runner.run_job(job)
                                self.registry.update_last_run(job.name, current_time)
                                
                                def _update_metrics(f, job_name):
                                    try:
                                        metrics = f.result()
                                        self.registry.add_execution_record(metrics)
                                    except Exception as e:
                                        self.logger.error(f"Failed to get job result for {job_name}: {str(e)}")
                                
                                future.add_done_callback(lambda f, jn=job.name: _update_metrics(f, jn))
                        
                        except Exception as e:
                            self.logger.error(f"Error processing job '{job.name}': {str(e)}")
                    
                    self._stop_event.wait(timeout=self._check_interval)
                
                except Exception as e:
                    self.logger.error(f"Error in scheduler loop: {str(e)}")
                    self._stop_event.wait(timeout=1.0)
        
        except Exception as e:
            self.logger.error(f"Scheduler loop failed: {str(e)}")
        finally:
            self.logger.info("Scheduler loop terminated")

    def get_job_status(self, job_name: str) -> Optional[Dict[str, Any]]:
        try:
            job = self.registry.get_job(job_name)
            if not job:
                return None
            
            is_running = self.runner.is_job_running(job_name)
            history = self.registry.get_execution_history(job_name, limit=5)
            
            return {
                "name": job.name,
                "enabled": job.enabled,
                "type": job.job_type.value,
                "is_running": is_running,
                "last_run_time": job.last_run_time.isoformat() if job.last_run_time else None,
                "recent_executions": [m.to_dict() for m in history]
            }
        except Exception as e:
            self.logger.error(f"Error getting job status for '{job_name}': {str(e)}")
            return None

    def get_all_jobs_status(self) -> List[Dict[str, Any]]:
        try:
            jobs = self.registry.get_all_jobs()
            statuses = []
            
            for job in jobs:
                status = self.get_job_status(job.name)
                if status:
                    statuses.append(status)
            
            return statuses
        except Exception as e:
            self.logger.error(f"Error getting all jobs status: {str(e)}")
            return []


class Scheduler:
    _instance = None
    _lock = threading.RLock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        self.logger = get_logger("Scheduler")
        
        self.registry = JobRegistry()
        self.runner = JobRunner(max_workers=5)
        self.scheduling_logic = SchedulingLogic()
        self.engine = SchedulerEngine(self.registry, self.runner, self.scheduling_logic)
        
        self._initialized = True
        self.logger.info("Scheduler singleton initialized")

    def register_job(self, name: str, func: Callable, job_type: JobType,
                    enabled: bool = True, interval_seconds: Optional[int] = None,
                    cron_expression: Optional[str] = None, max_retries: int = 3,
                    retry_delay_seconds: float = 2.0, timeout_seconds: Optional[float] = None,
                    priority: int = 5) -> None:
        try:
            config = JobConfig(
                name=name,
                func=func,
                job_type=job_type,
                enabled=enabled,
                interval_seconds=interval_seconds,
                cron_expression=cron_expression,
                max_retries=max_retries,
                retry_delay_seconds=retry_delay_seconds,
                timeout_seconds=timeout_seconds,
                priority=priority
            )
            self.registry.register(config)
        except Exception as e:
            self.logger.error(f"Failed to register job '{name}': {str(e)}")
            raise

    def start(self) -> None:
        self.engine.start()

    def stop(self, timeout: float = 30.0) -> None:
        try:
            self.engine.stop(timeout=timeout)
            self.runner.shutdown(wait=True, timeout=timeout)
            self.logger.info("Scheduler stopped gracefully")
        except Exception as e:
            self.logger.error(f"Error during scheduler shutdown: {str(e)}")
            raise

    def enable_job(self, job_name: str) -> None:
        self.registry.enable_job(job_name)

    def disable_job(self, job_name: str) -> None:
        self.registry.disable_job(job_name)

    def get_job_status(self, job_name: str) -> Optional[Dict[str, Any]]:
        return self.engine.get_job_status(job_name)

    def get_all_jobs_status(self) -> List[Dict[str, Any]]:
        return self.engine.get_all_jobs_status()

    def unregister_job(self, job_name: str) -> None:
        self.registry.unregister(job_name)
