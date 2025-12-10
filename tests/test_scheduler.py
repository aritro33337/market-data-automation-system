import pytest
import time
import threading
from datetime import datetime, timedelta
from src.core.scheduler import (
    Scheduler, JobType, JobStatus, JobRegistry, SchedulingLogic,
    JobRunner, SchedulerEngine, JobConfig, JobExecutionMetrics
)


class TestJobRegistry:
    def test_register_job(self):
        registry = JobRegistry()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30
        )
        
        registry.register(config)
        assert registry.get_job("test_job") is not None
    
    def test_register_duplicate_job(self):
        registry = JobRegistry()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30
        )
        
        registry.register(config)
        registry.register(config)
        assert len(registry.get_all_jobs()) == 1
    
    def test_unregister_job(self):
        registry = JobRegistry()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30
        )
        
        registry.register(config)
        registry.unregister("test_job")
        assert registry.get_job("test_job") is None
    
    def test_enable_disable_job(self):
        registry = JobRegistry()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30
        )
        
        registry.register(config)
        assert registry.get_job("test_job").enabled is True
        
        registry.disable_job("test_job")
        assert registry.get_job("test_job").enabled is False
        
        registry.enable_job("test_job")
        assert registry.get_job("test_job").enabled is True
    
    def test_validation_empty_name(self):
        registry = JobRegistry()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30
        )
        
        with pytest.raises(ValueError):
            registry.register(config)
    
    def test_validation_invalid_function(self):
        registry = JobRegistry()
        
        config = JobConfig(
            name="test_job",
            func=None,
            job_type=JobType.INTERVAL,
            interval_seconds=30
        )
        
        with pytest.raises(ValueError):
            registry.register(config)
    
    def test_validation_interval_required(self):
        registry = JobRegistry()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=None
        )
        
        with pytest.raises(ValueError):
            registry.register(config)
    
    def test_execution_history(self):
        registry = JobRegistry()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30
        )
        
        registry.register(config)
        
        metrics = JobExecutionMetrics(
            job_name="test_job",
            start_time=datetime.now(),
            status=JobStatus.COMPLETED
        )
        
        registry.add_execution_record(metrics)
        history = registry.get_execution_history("test_job")
        assert len(history) == 1


class TestSchedulingLogic:
    def test_interval_should_run(self):
        logic = SchedulingLogic()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30,
            last_run_time=datetime.now() - timedelta(seconds=35)
        )
        
        assert logic.should_run(config, datetime.now()) is True
    
    def test_interval_should_not_run(self):
        logic = SchedulingLogic()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30,
            last_run_time=datetime.now() - timedelta(seconds=10)
        )
        
        assert logic.should_run(config, datetime.now()) is False
    
    def test_cron_valid(self):
        logic = SchedulingLogic()
        
        def dummy_func():
            pass
        
        config = JobConfig(
            name="test_job",
            func=dummy_func,
            job_type=JobType.CRON,
            cron_expression="0 0 * * *"
        )
        
        assert logic.should_run(config, datetime.now()) is not None


class TestJobRunner:
    def test_run_simple_job(self):
        runner = JobRunner(max_workers=2)
        
        execution_log = []
        
        def test_func():
            execution_log.append("executed")
        
        config = JobConfig(
            name="test_job",
            func=test_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30
        )
        
        future = runner.run_job(config)
        metrics = future.result(timeout=5)
        
        assert metrics.status == JobStatus.COMPLETED
        runner.shutdown(wait=True, timeout=5)
    
    def test_job_timeout_handler(self):
        runner = JobRunner(max_workers=2)
        
        def slow_func():
            time.sleep(2)
        
        config = JobConfig(
            name="timeout_job",
            func=slow_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30,
            timeout_seconds=0.5,
            max_retries=0
        )
        
        future = runner.run_job(config)
        metrics = future.result(timeout=10)
        
        assert metrics.status == JobStatus.FAILED
        assert "timeout" in metrics.error_message.lower()
        runner.shutdown(wait=True, timeout=5)
    
    def test_job_with_retry(self):
        runner = JobRunner(max_workers=2)
        
        attempt_log = []
        
        def failing_func():
            attempt_log.append(1)
            if len(attempt_log) < 3:
                raise ValueError("Test error")
        
        config = JobConfig(
            name="retry_job",
            func=failing_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30,
            max_retries=3,
            retry_delay_seconds=0.1
        )
        
        future = runner.run_job(config)
        metrics = future.result(timeout=10)
        
        assert metrics.status == JobStatus.COMPLETED
        assert len(attempt_log) == 3
        runner.shutdown(wait=True, timeout=5)


class TestScheduler:
    def test_scheduler_singleton(self):
        scheduler1 = Scheduler()
        scheduler2 = Scheduler()
        
        assert scheduler1 is scheduler2
    
    def test_register_and_start_stop(self):
        scheduler = Scheduler()
        scheduler.registry._jobs.clear()
        
        def dummy_func():
            pass
        
        scheduler.register_job(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30,
            max_retries=1
        )
        
        scheduler.start()
        time.sleep(1)
        scheduler.stop(timeout=5)
        
        assert len(scheduler.registry.get_all_jobs()) == 1
    
    def test_hybrid_job_with_cron_only(self):
        scheduler = Scheduler()
        scheduler.registry._jobs.clear()
        
        def dummy_func():
            pass
        
        scheduler.register_job(
            name="hybrid_cron",
            func=dummy_func,
            job_type=JobType.HYBRID,
            cron_expression="*/5 * * * *",
            max_retries=1
        )
        
        job = scheduler.registry.get_job("hybrid_cron")
        assert job is not None
        assert job.job_type == JobType.HYBRID
    
    def test_hybrid_job_with_interval_only(self):
        scheduler = Scheduler()
        scheduler.registry._jobs.clear()
        
        def dummy_func():
            pass
        
        scheduler.register_job(
            name="hybrid_interval",
            func=dummy_func,
            job_type=JobType.HYBRID,
            interval_seconds=30,
            max_retries=1
        )
        
        job = scheduler.registry.get_job("hybrid_interval")
        assert job is not None
        assert job.job_type == JobType.HYBRID
    
    def test_hybrid_job_with_both(self):
        scheduler = Scheduler()
        scheduler.registry._jobs.clear()
        
        def dummy_func():
            pass
        
        scheduler.register_job(
            name="hybrid_both",
            func=dummy_func,
            job_type=JobType.HYBRID,
            interval_seconds=30,
            cron_expression="*/5 * * * *",
            max_retries=1
        )
        
        job = scheduler.registry.get_job("hybrid_both")
        assert job is not None
        assert job.interval_seconds == 30
        assert job.cron_expression == "*/5 * * * *"
    
    def test_hybrid_job_with_neither_fails(self):
        scheduler = Scheduler()
        scheduler.registry._jobs.clear()
        
        def dummy_func():
            pass
        
        with pytest.raises(ValueError):
            scheduler.register_job(
                name="hybrid_neither",
                func=dummy_func,
                job_type=JobType.HYBRID,
                max_retries=1
            )
    
    def test_multiple_jobs_concurrent(self):
        scheduler = Scheduler()
        scheduler.registry._jobs.clear()
        
        execution_log = []
        
        def job1():
            execution_log.append("job1")
        
        def job2():
            execution_log.append("job2")
        
        def job3():
            execution_log.append("job3")
        
        scheduler.register_job("job1", job1, JobType.INTERVAL, interval_seconds=1, max_retries=1)
        scheduler.register_job("job2", job2, JobType.INTERVAL, interval_seconds=1, max_retries=1)
        scheduler.register_job("job3", job3, JobType.INTERVAL, interval_seconds=1, max_retries=1)
        
        scheduler.start()
        time.sleep(3)
        scheduler.stop(timeout=5)
        
        assert len(scheduler.registry.get_all_jobs()) == 3
    
    def test_thread_safe_registration(self):
        scheduler = Scheduler()
        scheduler.registry._jobs.clear()
        
        errors = []
        
        def register_jobs():
            try:
                for i in range(10):
                    scheduler.register_job(
                        name=f"concurrent_{threading.current_thread().name}_{i}",
                        func=lambda: None,
                        job_type=JobType.INTERVAL,
                        interval_seconds=1,
                        max_retries=1
                    )
            except Exception as e:
                errors.append(str(e))
        
        threads = [threading.Thread(target=register_jobs, name=f"t{i}") for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert len(scheduler.registry.get_all_jobs()) == 50
    
    def test_job_enable_disable(self):
        scheduler = Scheduler()
        scheduler.registry._jobs.clear()
        
        def dummy_func():
            pass
        
        scheduler.register_job(
            name="test_job",
            func=dummy_func,
            job_type=JobType.INTERVAL,
            interval_seconds=30,
            max_retries=1
        )
        
        job = scheduler.registry.get_job("test_job")
        assert job.enabled is True
        
        scheduler.disable_job("test_job")
        assert scheduler.registry.get_job("test_job").enabled is False
        
        scheduler.enable_job("test_job")
        assert scheduler.registry.get_job("test_job").enabled is True


class TestTimeout:
    def test_timeout_thread_based(self):
        runner = JobRunner(max_workers=2)
        
        def quick_job():
            time.sleep(0.2)
            return "done"
        
        config = JobConfig(
            name="quick_job",
            func=quick_job,
            job_type=JobType.INTERVAL,
            interval_seconds=30,
            timeout_seconds=2,
            max_retries=0
        )
        
        future = runner.run_job(config)
        metrics = future.result(timeout=10)
        
        assert metrics.status == JobStatus.COMPLETED
        runner.shutdown(wait=True, timeout=5)
    
    def test_timeout_exception_propagation(self):
        runner = JobRunner(max_workers=2)
        
        def very_slow_job():
            time.sleep(5)
        
        config = JobConfig(
            name="slow_job",
            func=very_slow_job,
            job_type=JobType.INTERVAL,
            interval_seconds=30,
            timeout_seconds=0.5,
            max_retries=0
        )
        
        future = runner.run_job(config)
        metrics = future.result(timeout=10)
        
        assert metrics.status == JobStatus.FAILED
        assert "timeout" in metrics.error_message.lower()
        runner.shutdown(wait=True, timeout=5)


class TestEdgeCases:
    def test_empty_job_name_rejected(self):
        scheduler = Scheduler()
        
        with pytest.raises(ValueError):
            scheduler.register_job(
                name="",
                func=lambda: None,
                job_type=JobType.INTERVAL,
                interval_seconds=30
            )
    
    def test_negative_interval_rejected(self):
        scheduler = Scheduler()
        
        with pytest.raises(ValueError):
            scheduler.register_job(
                name="bad_job",
                func=lambda: None,
                job_type=JobType.INTERVAL,
                interval_seconds=-10
            )
    
    def test_zero_interval_rejected(self):
        scheduler = Scheduler()
        
        with pytest.raises(ValueError):
            scheduler.register_job(
                name="bad_job",
                func=lambda: None,
                job_type=JobType.INTERVAL,
                interval_seconds=0
            )
    
    def test_invalid_cron_rejected(self):
        scheduler = Scheduler()
        
        with pytest.raises(ValueError):
            scheduler.register_job(
                name="bad_cron",
                func=lambda: None,
                job_type=JobType.CRON,
                cron_expression="invalid cron"
            )
    
    def test_nonexistent_job_status(self):
        scheduler = Scheduler()
        
        status = scheduler.get_job_status("nonexistent")
        assert status is None
    
    def test_max_retries_zero(self):
        scheduler = Scheduler()
        scheduler.registry._jobs.clear()
        
        attempt_count = []
        
        def failing_job():
            attempt_count.append(1)
            raise ValueError("Test")
        
        scheduler.register_job(
            name="no_retry_job",
            func=failing_job,
            job_type=JobType.INTERVAL,
            interval_seconds=30,
            max_retries=0
        )
        
        scheduler.start()
        time.sleep(1)
        scheduler.stop(timeout=5)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
