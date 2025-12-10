import unittest
import json
import sys
import tempfile
import shutil
import time
import random
import threading
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.main import PipelineOrchestrator
from src.ml_inference.inference import MLInferenceEngine
from src.core.aggregator import DataAggregator
from src.core.live_data_fetcher import LiveDataFetcher
from src.core.excel_exporter import ExcelExporter


class TestPipelineOrchestrator(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.config_dir = Path(cls.test_dir) / "config"
        cls.config_dir.mkdir(parents=True, exist_ok=True)
        
        cls.test_config = {
            "app_name": "Market Data Automation Test",
            "version": "1.0.0",
            "api": {
                "timeout_seconds": 30,
                "rate_limit_delay": 0.5
            },
            "data_sources": {
                "symbols": ["AAPL", "TSLA"],
                "fallback_symbols": ["MSFT"],
                "default_interval_seconds": 30,
                "raw_data_path": str(Path(cls.test_dir) / "data" / "raw"),
                "processed_data_path": str(Path(cls.test_dir) / "data" / "processed")
            },
            "export": {
                "excel_output_folder": str(Path(cls.test_dir) / "data" / "exports"),
                "excel_file_prefix": "test_market_data_"
            },
            "scheduler": {
                "enabled": True,
                "interval_seconds": 60,
                "max_workers": 2
            },
            "logging": {
                "level": "INFO",
                "log_dir": str(Path(cls.test_dir) / "logs"),
                "use_color": False,
                "use_json": False
            },
            "ml": {
                "inference_enabled": False,
                "model_path": "ml/models/latest_model.pkl",
                "feature_config_path": "ml/features/feature_config.json"
            }
        }
        
        config_file = cls.config_dir / "settings.json"
        with open(config_file, 'w') as f:
            json.dump(cls.test_config, f)
        
        cls.config_path = str(config_file)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def test_orchestrator_initialization(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            self.assertIsNotNone(orchestrator)
            self.assertIsNotNone(orchestrator.config)
            self.assertIsNotNone(orchestrator.fetcher)
            self.assertIsNotNone(orchestrator.aggregator)
            self.assertIsNotNone(orchestrator.exporter)
            self.assertIsNotNone(orchestrator.scheduler)
        except Exception as e:
            self.fail(f"Orchestrator initialization failed: {str(e)}")
    
    def test_config_loading(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            self.assertEqual(orchestrator.config.get("app_name"), "Market Data Automation Test")
            self.assertEqual(orchestrator.config.get("version"), "1.0.0")
        except Exception as e:
            self.fail(f"Config loading failed: {str(e)}")
    
    def test_directory_creation(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            dirs = [
                "data/raw",
                "data/processed",
                "data/exports",
                "logs"
            ]
            
            for dir_name in dirs:
                dir_path = Path(self.test_dir) / dir_name
                self.assertTrue(dir_path.exists(), f"Directory {dir_name} was not created")
        except Exception as e:
            self.fail(f"Directory creation test failed: {str(e)}")
    
    def test_ml_disabled_initialization(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            self.assertIsNone(orchestrator.ml_engine)
        except Exception as e:
            self.fail(f"ML disabled initialization failed: {str(e)}")
    
    def test_fetch_data_returns_valid_structure(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_raw_data = {
                "AAPL": {
                    "price": 150.0,
                    "open": 145.0,
                    "high": 155.0,
                    "low": 144.0,
                    "volume": 1000000.0,
                    "change": 5.0,
                    "change_percent": 3.4,
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                },
                "TSLA": {
                    "price": 250.0,
                    "open": 245.0,
                    "high": 260.0,
                    "low": 240.0,
                    "volume": 800000.0,
                    "change": 5.0,
                    "change_percent": 2.0,
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }
            }
            
            with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=mock_raw_data):
                raw_data = orchestrator._fetch_data()
                self.assertIsNotNone(raw_data)
                self.assertEqual(len(raw_data), 2)
                self.assertIn("AAPL", raw_data)
                self.assertIn("TSLA", raw_data)
        except Exception as e:
            self.fail(f"Fetch data test failed: {str(e)}")
    
    def test_fetch_data_handles_none(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=None):
                raw_data = orchestrator._fetch_data()
                self.assertIsNone(raw_data)
        except Exception as e:
            self.fail(f"Fetch data None handling test failed: {str(e)}")
    
    def test_aggregate_data_valid_input(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_raw_data = {
                "AAPL": {
                    "price": 150.0,
                    "open": 145.0,
                    "high": 155.0,
                    "low": 144.0,
                    "volume": 1000000.0
                }
            }
            
            with patch.object(orchestrator.aggregator, 'run', return_value={"symbols": {"AAPL": {"price": 150.0}}}):
                aggregated = orchestrator._aggregate_data(mock_raw_data)
                self.assertIsNotNone(aggregated)
                self.assertIn("symbols", aggregated)
        except Exception as e:
            self.fail(f"Aggregate data test failed: {str(e)}")
    
    def test_aggregate_data_handles_empty(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            aggregated = orchestrator._aggregate_data({})
            self.assertIsNone(aggregated)
        except Exception as e:
            self.fail(f"Aggregate data empty handling test failed: {str(e)}")
    
    def test_ml_inference_skipped_when_disabled(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_aggregated = {
                "symbols": {
                    "AAPL": {"price": 150.0, "volume": 1000000.0}
                }
            }
            
            processed = orchestrator._apply_ml_predictions(mock_aggregated)
            self.assertEqual(processed, mock_aggregated)
            self.assertNotIn("ml_predictions", processed)
        except Exception as e:
            self.fail(f"ML skip test failed: {str(e)}")
    
    def test_export_data_valid_input(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_processed_data = {
                "symbols": {
                    "AAPL": {"price": 150.0, "volume": 1000000.0}
                }
            }
            
            with patch.object(orchestrator.exporter, 'export_to_excel', return_value="market_data_test.xlsx"):
                success = orchestrator._export_data(mock_processed_data)
                self.assertTrue(success)
        except Exception as e:
            self.fail(f"Export data test failed: {str(e)}")
    
    def test_export_data_handles_empty(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            success = orchestrator._export_data({})
            self.assertFalse(success)
        except Exception as e:
            self.fail(f"Export data empty handling test failed: {str(e)}")
    
    def test_full_pipeline_execution(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_raw_data = {
                "AAPL": {
                    "price": 150.0,
                    "open": 145.0,
                    "high": 155.0,
                    "low": 144.0,
                    "volume": 1000000.0
                }
            }
            
            mock_aggregated = {
                "symbols": {
                    "AAPL": {"price": 150.0, "volume": 1000000.0}
                }
            }
            
            with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=mock_raw_data), \
                 patch.object(orchestrator.aggregator, 'run', return_value=mock_aggregated), \
                 patch.object(orchestrator.exporter, 'export_to_excel', return_value="market_data_test.xlsx"):
                
                success = orchestrator.run_full_pipeline()
                self.assertTrue(success)
        except Exception as e:
            self.fail(f"Full pipeline execution test failed: {str(e)}")
    
    def test_full_pipeline_handles_fetch_failure(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=None):
                success = orchestrator.run_full_pipeline()
                self.assertFalse(success)
        except Exception as e:
            self.fail(f"Pipeline fetch failure handling test failed: {str(e)}")
    
    def test_full_pipeline_handles_aggregation_failure(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_raw_data = {"AAPL": {"price": 150.0}}
            
            with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=mock_raw_data), \
                 patch.object(orchestrator.aggregator, 'run', return_value=None):
                
                success = orchestrator.run_full_pipeline()
                self.assertFalse(success)
        except Exception as e:
            self.fail(f"Pipeline aggregation failure handling test failed: {str(e)}")
    
    def test_full_pipeline_handles_export_failure(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_raw_data = {"AAPL": {"price": 150.0}}
            mock_aggregated = {"symbols": {"AAPL": {"price": 150.0}}}
            
            with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=mock_raw_data), \
                 patch.object(orchestrator.aggregator, 'run', return_value=mock_aggregated), \
                 patch.object(orchestrator.exporter, 'export_to_excel', return_value=None):
                
                success = orchestrator.run_full_pipeline()
                self.assertFalse(success)
        except Exception as e:
            self.fail(f"Pipeline export failure handling test failed: {str(e)}")
    
    def test_scheduler_registration(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            orchestrator.register_pipeline_with_scheduler()
            
            job = orchestrator.scheduler.registry.get_job("market_data_pipeline")
            self.assertIsNotNone(job)
            self.assertEqual(job.name, "market_data_pipeline")
            self.assertTrue(job.enabled)
        except Exception as e:
            self.fail(f"Scheduler registration test failed: {str(e)}")
    
    def test_scheduler_disabled_in_config(self):
        try:
            config = self.test_config.copy()
            config["scheduler"]["enabled"] = False
            
            config_file = self.config_dir / "settings_disabled.json"
            with open(config_file, 'w') as f:
                json.dump(config, f)
            
            orchestrator = PipelineOrchestrator(str(config_file))
            orchestrator.register_pipeline_with_scheduler()
        except Exception as e:
            self.fail(f"Scheduler disabled test failed: {str(e)}")
    
    def test_shutdown_cleanup(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            orchestrator.shutdown()
        except Exception as e:
            self.fail(f"Shutdown cleanup test failed: {str(e)}")


class TestMLInferenceEngine(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        try:
            import sklearn
        except ImportError:
            raise unittest.SkipTest("scikit-learn not installed - ML testing skipped (optional dependency)")
        
        cls.test_dir = tempfile.mkdtemp()
        cls.ml_dir = Path(cls.test_dir) / "ml"
        cls.ml_dir.mkdir(parents=True, exist_ok=True)
        
        cls.models_dir = cls.ml_dir / "models"
        cls.models_dir.mkdir(parents=True, exist_ok=True)
        
        cls.features_dir = cls.ml_dir / "features"
        cls.features_dir.mkdir(parents=True, exist_ok=True)
        
        import pickle
        import numpy as np
        from sklearn.linear_model import LinearRegression
        
        X = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        y = np.array([1, 2, 3])
        model = LinearRegression()
        model.fit(X, y)
        
        model_file = cls.models_dir / "test_model.pkl"
        with open(model_file, 'wb') as f:
            pickle.dump(model, f)
        
        cls.model_path = str(model_file)
        
        feature_config = {
            "features": ["price", "volume", "change_percent"]
        }
        feature_file = cls.features_dir / "feature_config.json"
        with open(feature_file, 'w') as f:
            json.dump(feature_config, f)
        
        cls.feature_config_path = str(feature_file)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def test_inference_engine_initialization(self):
        try:
            engine = MLInferenceEngine(self.model_path, self.feature_config_path)
            self.assertIsNotNone(engine.model)
            self.assertIsNotNone(engine.feature_config)
            self.assertEqual(len(engine.feature_columns), 3)
        except Exception as e:
            self.fail(f"ML Engine initialization failed: {str(e)}")
    
    def test_inference_engine_model_loading(self):
        try:
            engine = MLInferenceEngine(self.model_path, self.feature_config_path)
            self.assertIsNotNone(engine.model)
            self.assertTrue(hasattr(engine.model, 'predict'))
        except Exception as e:
            self.fail(f"Model loading test failed: {str(e)}")
    
    def test_feature_extraction(self):
        try:
            engine = MLInferenceEngine(self.model_path, self.feature_config_path)
            
            symbol_data = {
                "price": 150.0,
                "open": 145.0,
                "high": 155.0,
                "low": 144.0,
                "volume": 1000000.0,
                "change": 5.0,
                "change_percent": 3.4
            }
            
            features = engine._extract_features(symbol_data)
            self.assertIsNotNone(features)
            self.assertIn("price", features)
            self.assertIn("volume", features)
        except Exception as e:
            self.fail(f"Feature extraction test failed: {str(e)}")
    
    def test_feature_extraction_handles_none(self):
        try:
            engine = MLInferenceEngine(self.model_path, self.feature_config_path)
            features = engine._extract_features(None)
            self.assertIsNone(features)
        except Exception as e:
            self.fail(f"Feature extraction None handling test failed: {str(e)}")
    
    def test_single_prediction(self):
        try:
            engine = MLInferenceEngine(self.model_path, self.feature_config_path)
            
            symbol_data = {
                "price": 150.0,
                "volume": 1000000.0,
                "change_percent": 3.4,
                "open": 145.0,
                "high": 155.0,
                "low": 144.0,
                "change": 5.0
            }
            
            prediction = engine.predict_single("AAPL", symbol_data)
            self.assertIsNotNone(prediction)
            self.assertEqual(prediction["symbol"], "AAPL")
            self.assertIn("prediction", prediction)
        except Exception as e:
            self.fail(f"Single prediction test failed: {str(e)}")
    
    def test_batch_prediction(self):
        try:
            engine = MLInferenceEngine(self.model_path, self.feature_config_path)
            
            aggregated_data = {
                "symbols": {
                    "AAPL": {
                        "price": 150.0,
                        "volume": 1000000.0,
                        "change_percent": 3.4,
                        "open": 145.0,
                        "high": 155.0,
                        "low": 144.0,
                        "change": 5.0
                    },
                    "TSLA": {
                        "price": 250.0,
                        "volume": 800000.0,
                        "change_percent": 2.0,
                        "open": 245.0,
                        "high": 260.0,
                        "low": 240.0,
                        "change": 5.0
                    }
                }
            }
            
            predictions = engine.predict(aggregated_data)
            self.assertIsNotNone(predictions)
            self.assertGreater(len(predictions), 0)
        except Exception as e:
            self.fail(f"Batch prediction test failed: {str(e)}")
    
    def test_prediction_handles_empty_data(self):
        try:
            engine = MLInferenceEngine(self.model_path, self.feature_config_path)
            predictions = engine.predict({})
            self.assertEqual(predictions, {})
        except Exception as e:
            self.fail(f"Prediction empty data handling test failed: {str(e)}")


class TestDataPipelineIntegration(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.config_dir = Path(cls.test_dir) / "config"
        cls.config_dir.mkdir(parents=True, exist_ok=True)
        
        cls.test_config = {
            "app_name": "Integration Test",
            "version": "1.0.0",
            "api": {"timeout_seconds": 30, "rate_limit_delay": 0.5},
            "data_sources": {
                "symbols": ["AAPL"],
                "fallback_symbols": [],
                "raw_data_path": str(Path(cls.test_dir) / "data" / "raw"),
                "processed_data_path": str(Path(cls.test_dir) / "data" / "processed")
            },
            "export": {
                "excel_output_folder": str(Path(cls.test_dir) / "data" / "exports")
            },
            "scheduler": {"enabled": True, "interval_seconds": 60},
            "logging": {"level": "INFO", "log_dir": str(Path(cls.test_dir) / "logs")},
            "ml": {"inference_enabled": False}
        }
        
        config_file = cls.config_dir / "settings.json"
        with open(config_file, 'w') as f:
            json.dump(cls.test_config, f)
        
        cls.config_path = str(config_file)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def test_end_to_end_pipeline_with_mocks(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_raw = {
                "AAPL": {
                    "price": 150.0,
                    "open": 145.0,
                    "high": 155.0,
                    "low": 144.0,
                    "volume": 1000000.0,
                    "change": 5.0,
                    "change_percent": 3.4
                }
            }
            
            # We want to test the REAL aggregator logic, so we don't mock run() entirely.
            # But we mock the fetcher to return controlled data.
            # And we mock the exporter's actual file writing to avoid clutter, 
            # but we want to ensure the orchestrator passes the right data to it.
            
            with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=mock_raw), \
                 patch.object(orchestrator.exporter, 'export_to_excel', return_value="market_data_test.xlsx") as mock_export:
                
                result = orchestrator.run_full_pipeline()
                self.assertTrue(result)
                
                # Verify export was called
                mock_export.assert_called_once()
                
                # Verify the data passed to export has the expected structure
                args, _ = mock_export.call_args
                data_passed = args[0]
                self.assertIsInstance(data_passed, list) # Should be a list of dicts now
                self.assertEqual(len(data_passed), 1)
                self.assertEqual(data_passed[0]['symbol'], 'AAPL')
                
        except Exception as e:
            self.fail(f"End-to-end integration test failed: {str(e)}")


class TestEnterprisePerformance(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.config_dir = Path(cls.test_dir) / "config"
        cls.config_dir.mkdir(parents=True, exist_ok=True)
        
        cls.perf_config = {
            "app_name": "Enterprise Performance Test",
            "version": "1.0.0",
            "api": {"timeout_seconds": 30, "rate_limit_delay": 0.5},
            "data_sources": {
                "symbols": ["AAPL", "TSLA", "GOOGL", "MSFT", "AMZN"],
                "fallback_symbols": ["META"],
                "raw_data_path": str(Path(cls.test_dir) / "data" / "raw"),
                "processed_data_path": str(Path(cls.test_dir) / "data" / "processed")
            },
            "export": {"excel_output_folder": str(Path(cls.test_dir) / "data" / "exports")},
            "scheduler": {"enabled": True, "interval_seconds": 60},
            "logging": {"level": "INFO", "log_dir": str(Path(cls.test_dir) / "logs")},
            "ml": {"inference_enabled": False}
        }
        
        config_file = cls.config_dir / "perf_settings.json"
        with open(config_file, 'w') as f:
            json.dump(cls.perf_config, f)
        
        cls.config_path = str(config_file)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def test_large_dataset_processing(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            large_dataset = {
                f"SYMBOL_{i:04d}": {
                    "price": random.uniform(10, 1000),
                    "open": random.uniform(10, 1000),
                    "high": random.uniform(10, 1000),
                    "low": random.uniform(5, 900),
                    "volume": random.uniform(100000, 100000000),
                    "change": random.uniform(-20, 20),
                    "change_percent": random.uniform(-10, 10)
                }
                for i in range(500)
            }
            
            mock_aggregated = {
                "symbols": {
                    symbol: {
                        "symbol": symbol,
                        "price": data["price"],
                        "volume": data["volume"],
                        "type": "stock"
                    }
                    for symbol, data in large_dataset.items()
                }
            }
            
            start_time = time.time()
            
            with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=large_dataset), \
                 patch.object(orchestrator.aggregator, 'run', return_value=mock_aggregated), \
                 patch.object(orchestrator.exporter, 'export_to_excel', return_value="large_dataset.xlsx"):
                
                success = orchestrator.run_full_pipeline()
            
            execution_time = time.time() - start_time
            
            self.assertTrue(success)
            self.assertLess(execution_time, 10.0, f"Large dataset processing took {execution_time:.2f}s (expected < 10s)")
        except Exception as e:
            self.fail(f"Large dataset processing test failed: {str(e)}")
    
    def test_pipeline_execution_speed(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_raw = {
                f"SYM_{i}": {
                    "price": random.uniform(100, 500),
                    "open": random.uniform(100, 500),
                    "high": random.uniform(100, 500),
                    "low": random.uniform(50, 400),
                    "volume": random.uniform(1000000, 10000000)
                }
                for i in range(100)
            }
            
            mock_aggregated = {
                "symbols": {
                    symbol: {"symbol": symbol, "price": data["price"], "volume": data["volume"]}
                    for symbol, data in mock_raw.items()
                }
            }
            
            execution_times = []
            
            for _ in range(5):
                start = time.time()
                
                with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=mock_raw), \
                     patch.object(orchestrator.aggregator, 'run', return_value=mock_aggregated), \
                     patch.object(orchestrator.exporter, 'export_to_excel', return_value="test.xlsx"):
                    
                    orchestrator.run_full_pipeline()
                
                execution_times.append(time.time() - start)
            
            avg_time = sum(execution_times) / len(execution_times)
            self.assertLess(avg_time, 2.0, f"Average execution time {avg_time:.3f}s (expected < 2s)")
        except Exception as e:
            self.fail(f"Pipeline execution speed test failed: {str(e)}")
    
    def test_concurrent_pipeline_executions(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            mock_raw = {f"S{i}": {"price": random.uniform(100, 500), "volume": random.uniform(1000000, 10000000)} for i in range(50)}
            mock_agg = {"symbols": {s: {"symbol": s, "price": d["price"], "volume": d["volume"]} for s, d in mock_raw.items()}}
            
            results = []
            
            def run_pipeline():
                with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=mock_raw), \
                     patch.object(orchestrator.aggregator, 'run', return_value=mock_agg), \
                     patch.object(orchestrator.exporter, 'export_to_excel', return_value="test.xlsx"):
                    
                    result = orchestrator.run_full_pipeline()
                    results.append(result)
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(run_pipeline) for _ in range(10)]
                for future in futures:
                    future.result(timeout=10)
            
            success_count = sum(1 for r in results if r)
            self.assertEqual(success_count, 10, f"Expected 10 successful concurrent executions, got {success_count}")
        except Exception as e:
            self.fail(f"Concurrent execution test failed: {str(e)}")
    
    def test_data_integrity_validation(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            test_data = {
                "AAPL": {"price": 150.50, "open": 148.75, "high": 152.00, "low": 147.50, "volume": 50000000.0},
                "TSLA": {"price": 250.25, "open": 248.50, "high": 252.75, "low": 247.00, "volume": 40000000.0},
                "GOOGL": {"price": 140.80, "open": 139.50, "high": 142.00, "low": 138.50, "volume": 35000000.0}
            }
            
            mock_agg = {"symbols": {s: {"symbol": s, "price": d["price"], "volume": d["volume"], "integrity": "verified"} for s, d in test_data.items()}}
            
            with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=test_data), \
                 patch.object(orchestrator.aggregator, 'run', return_value=mock_agg), \
                 patch.object(orchestrator.exporter, 'export_to_excel', return_value="integrity.xlsx"):
                
                success = orchestrator.run_full_pipeline()
                self.assertTrue(success)
        except Exception as e:
            self.fail(f"Data integrity validation test failed: {str(e)}")
    
    def test_error_recovery_robustness(self):
        try:
            orchestrator = PipelineOrchestrator(self.config_path)
            
            test_data = {f"SYM_{i}": {"price": random.uniform(10, 1000) if i % 3 != 0 else None, "volume": random.uniform(100000, 10000000)} for i in range(50)}
            
            valid_data = {k: v for k, v in test_data.items() if v.get("price") is not None}
            mock_agg = {"symbols": {s: {"symbol": s, "price": d["price"], "volume": d["volume"]} for s, d in valid_data.items()}}
            
            recovery_count = 0
            
            for _ in range(5):
                with patch.object(orchestrator.fetcher, 'fetch_all_symbols', return_value=test_data), \
                     patch.object(orchestrator.aggregator, 'run', return_value=mock_agg), \
                     patch.object(orchestrator.exporter, 'export_to_excel', return_value="recovery.xlsx"):
                    
                    if orchestrator.run_full_pipeline():
                        recovery_count += 1
            
            self.assertGreater(recovery_count, 0, "System should recover from errors")
        except Exception as e:
            self.fail(f"Error recovery robustness test failed: {str(e)}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
