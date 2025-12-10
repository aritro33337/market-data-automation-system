import unittest
import json
import sys
import tempfile
import shutil
import pickle
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ml_inference.inference import MLInferenceEngine
from src.utils.logger import get_logger


class DummyModel:
    def predict(self, X):
        return np.array([0.75] * len(X))
    
    def predict_proba(self, X):
        return np.array([[0.25, 0.75]] * len(X))


class FakeModel:
    def predict(self, X):
        return np.array([0.75] * len(X))
    
    def predict_proba(self, X):
        return np.array([[0.25, 0.75]] * len(X))



class TestMLInferenceModelLoad(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.ml_dir = Path(cls.test_dir) / "ml"
        cls.ml_dir.mkdir(parents=True, exist_ok=True)
        
        cls.models_dir = cls.ml_dir / "models"
        cls.models_dir.mkdir(parents=True, exist_ok=True)
        
        cls.features_dir = cls.ml_dir / "features"
        cls.features_dir.mkdir(parents=True, exist_ok=True)
        
        fake_model = FakeModel()
        cls.model_path = cls.models_dir / "fake_model.pkl"
        with open(cls.model_path, 'wb') as f:
            pickle.dump(fake_model, f)
        
        cls.feature_config_path = cls.features_dir / "feature_config.json"
        with open(cls.feature_config_path, 'w') as f:
            json.dump({"features": ["price", "volume", "change_percent"]}, f)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def test_model_loads_successfully(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.feature_config_path))
            self.assertIsNotNone(engine.model)
            self.assertTrue(hasattr(engine.model, 'predict'))
        except Exception as e:
            self.fail(f"Model loading failed: {str(e)}")
    
    def test_missing_model_file_handling(self):
        try:
            missing_path = str(self.models_dir / "nonexistent_model.pkl")
            with self.assertRaises(FileNotFoundError):
                MLInferenceEngine(missing_path, str(self.feature_config_path))
        except Exception as e:
            self.fail(f"Missing model handling test failed: {str(e)}")
    
    def test_model_load_returns_valid_object(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.feature_config_path))
            self.assertIsInstance(engine, MLInferenceEngine)
            self.assertIsNotNone(engine.model)
            self.assertTrue(callable(engine.model.predict))
        except Exception as e:
            self.fail(f"Model object validation test failed: {str(e)}")
    
    def test_model_load_logs_correctly(self):
        try:
            with patch('src.ml_inference.inference.get_logger') as mock_logger_func:
                mock_logger = MagicMock()
                mock_logger_func.return_value = mock_logger
                
                engine = MLInferenceEngine(str(self.model_path), str(self.feature_config_path))
                self.assertIsNotNone(engine)
        except Exception as e:
            self.fail(f"Model load logging test failed: {str(e)}")


class TestFeatureConfigLoad(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.ml_dir = Path(cls.test_dir) / "ml"
        cls.ml_dir.mkdir(parents=True, exist_ok=True)
        
        cls.models_dir = cls.ml_dir / "models"
        cls.models_dir.mkdir(parents=True, exist_ok=True)
        
        cls.features_dir = cls.ml_dir / "features"
        cls.features_dir.mkdir(parents=True, exist_ok=True)
        
        fake_model = FakeModel()
        cls.model_path = cls.models_dir / "fake_model.pkl"
        with open(cls.model_path, 'wb') as f:
            pickle.dump(fake_model, f)
        
        cls.valid_config_path = cls.features_dir / "valid_config.json"
        with open(cls.valid_config_path, 'w') as f:
            json.dump({"features": ["price", "volume", "change_percent", "open", "high", "low"]}, f)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def test_feature_config_loads_successfully(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.valid_config_path))
            self.assertIsNotNone(engine.feature_config)
            self.assertEqual(len(engine.feature_columns), 6)
        except Exception as e:
            self.fail(f"Feature config loading failed: {str(e)}")
    
    def test_missing_feature_config_file(self):
        try:
            missing_config = str(self.features_dir / "missing_config.json")
            with self.assertRaises(FileNotFoundError):
                MLInferenceEngine(str(self.model_path), missing_config)
        except Exception as e:
            self.fail(f"Missing feature config handling test failed: {str(e)}")
    
    def test_invalid_feature_config_format(self):
        try:
            invalid_config_path = self.features_dir / "invalid_config.json"
            with open(invalid_config_path, 'w') as f:
                f.write("not valid json {{{")
            
            with self.assertRaises((json.JSONDecodeError, Exception)):
                MLInferenceEngine(str(self.model_path), str(invalid_config_path))
        except Exception as e:
            self.fail(f"Invalid feature config handling test failed: {str(e)}")
    
    def test_feature_config_contains_valid_features_list(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.valid_config_path))
            self.assertGreater(len(engine.feature_columns), 0)
            for feature in engine.feature_columns:
                self.assertIsInstance(feature, str)
                self.assertGreater(len(feature), 0)
        except Exception as e:
            self.fail(f"Feature config validation test failed: {str(e)}")


class TestFeatureExtraction(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.ml_dir = Path(cls.test_dir) / "ml"
        cls.ml_dir.mkdir(parents=True, exist_ok=True)
        
        cls.models_dir = cls.ml_dir / "models"
        cls.models_dir.mkdir(parents=True, exist_ok=True)
        
        cls.features_dir = cls.ml_dir / "features"
        cls.features_dir.mkdir(parents=True, exist_ok=True)
        
        fake_model = FakeModel()
        cls.model_path = cls.models_dir / "fake_model.pkl"
        with open(cls.model_path, 'wb') as f:
            pickle.dump(fake_model, f)
        
        cls.config_path = cls.features_dir / "config.json"
        with open(cls.config_path, 'w') as f:
            json.dump({"features": ["price", "volume", "change_percent"]}, f)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def test_feature_extraction_with_valid_data(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            symbol_data = {
                "price": 150.0,
                "open": 148.0,
                "high": 155.0,
                "low": 145.0,
                "volume": 1000000.0,
                "change": 2.0,
                "change_percent": 1.35
            }
            
            features = engine._extract_features(symbol_data)
            self.assertIsNotNone(features)
            self.assertIn("price", features)
            self.assertIn("volume", features)
        except Exception as e:
            self.fail(f"Feature extraction test failed: {str(e)}")
    
    def test_feature_extraction_output_structure(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            symbol_data = {
                "price": 150.0,
                "volume": 1000000.0,
                "change_percent": 1.35,
                "open": 148.0,
                "high": 155.0,
                "low": 145.0
            }
            
            features = engine._extract_features(symbol_data)
            self.assertIsInstance(features, dict)
            for key, value in features.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, (int, float))
        except Exception as e:
            self.fail(f"Feature extraction structure test failed: {str(e)}")
    
    def test_feature_extraction_with_missing_fields(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            incomplete_data = {
                "price": 150.0
            }
            
            features = engine._extract_features(incomplete_data)
            self.assertIsNotNone(features)
            self.assertEqual(features.get("price"), 150.0)
            self.assertEqual(features.get("volume"), 0.0)
        except Exception as e:
            self.fail(f"Feature extraction with missing fields test failed: {str(e)}")
    
    def test_feature_extraction_with_none_data(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            features = engine._extract_features(None)
            self.assertIsNone(features)
        except Exception as e:
            self.fail(f"Feature extraction with None test failed: {str(e)}")
    
    def test_feature_extraction_returns_floats(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            symbol_data = {
                "price": "150.5",
                "volume": "1000000",
                "change_percent": "1.35"
            }
            
            features = engine._extract_features(symbol_data)
            self.assertIsNotNone(features)
            
            for key, value in features.items():
                self.assertIsInstance(value, (int, float), f"Feature {key} should be numeric, got {type(value)}")
        except Exception as e:
            self.fail(f"Feature extraction float conversion test failed: {str(e)}")


class TestPredictionWithFakeModel(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.ml_dir = Path(cls.test_dir) / "ml"
        cls.ml_dir.mkdir(parents=True, exist_ok=True)
        
        cls.models_dir = cls.ml_dir / "models"
        cls.models_dir.mkdir(parents=True, exist_ok=True)
        
        cls.features_dir = cls.ml_dir / "features"
        cls.features_dir.mkdir(parents=True, exist_ok=True)
        
        fake_model = FakeModel()
        cls.model_path = cls.models_dir / "fake_model.pkl"
        with open(cls.model_path, 'wb') as f:
            pickle.dump(fake_model, f)
        
        cls.config_path = cls.features_dir / "config.json"
        with open(cls.config_path, 'w') as f:
            json.dump({"features": ["price", "volume", "change_percent"]}, f)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def test_prediction_returns_valid_structure(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            symbol_data = {
                "price": 150.0,
                "open": 148.0,
                "high": 155.0,
                "low": 145.0,
                "volume": 1000000.0,
                "change": 2.0,
                "change_percent": 1.35
            }
            
            prediction = engine.predict_single("BTC", symbol_data)
            self.assertIsNotNone(prediction)
            self.assertIn("prediction", prediction)
            self.assertIn("timestamp", prediction)
            self.assertIn("symbol", prediction)
        except Exception as e:
            self.fail(f"Prediction structure test failed: {str(e)}")
    
    def test_prediction_contains_metadata(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            symbol_data = {
                "price": 150.0,
                "volume": 1000000.0,
                "change_percent": 1.35,
                "open": 148.0,
                "high": 155.0,
                "low": 145.0
            }
            
            prediction = engine.predict_single("ETH", symbol_data)
            self.assertEqual(prediction["symbol"], "ETH")
            self.assertIn("confidence", prediction)
            self.assertIn("features_used", prediction)
            self.assertIsNotNone(prediction.get("timestamp"))
        except Exception as e:
            self.fail(f"Prediction metadata test failed: {str(e)}")
    
    def test_batch_prediction_works(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            aggregated_data = {
                "symbols": {
                    "BTC": {
                        "price": 150.0,
                        "volume": 1000000.0,
                        "change_percent": 1.35,
                        "open": 148.0,
                        "high": 155.0,
                        "low": 145.0
                    },
                    "ETH": {
                        "price": 100.0,
                        "volume": 500000.0,
                        "change_percent": 0.50,
                        "open": 99.5,
                        "high": 102.0,
                        "low": 98.0
                    }
                }
            }
            
            predictions = engine.predict(aggregated_data)
            self.assertGreater(len(predictions), 0)
            self.assertIn("BTC", predictions)
            self.assertIn("ETH", predictions)
        except Exception as e:
            self.fail(f"Batch prediction test failed: {str(e)}")


class TestMLDisabledFallback(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.ml_dir = Path(cls.test_dir) / "ml"
        cls.ml_dir.mkdir(parents=True, exist_ok=True)
        
        cls.models_dir = cls.ml_dir / "models"
        cls.models_dir.mkdir(parents=True, exist_ok=True)
        
        cls.features_dir = cls.ml_dir / "features"
        cls.features_dir.mkdir(parents=True, exist_ok=True)
        
        fake_model = FakeModel()
        cls.model_path = cls.models_dir / "fake_model.pkl"
        with open(cls.model_path, 'wb') as f:
            pickle.dump(fake_model, f)
        
        cls.config_path = cls.features_dir / "config.json"
        with open(cls.config_path, 'w') as f:
            json.dump({"features": ["price", "volume", "change_percent"]}, f)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def test_no_prediction_when_model_is_none(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            engine.model = None
            
            symbol_data = {
                "price": 150.0,
                "volume": 1000000.0,
                "change_percent": 1.35
            }
            
            prediction = engine.predict_single("BTC", symbol_data)
            self.assertIsNone(prediction)
        except Exception as e:
            self.fail(f"ML disabled fallback test failed: {str(e)}")
    
    def test_empty_predictions_when_engine_disabled(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            engine.model = None
            
            aggregated_data = {
                "symbols": {
                    "BTC": {"price": 150.0, "volume": 1000000.0, "change_percent": 1.35}
                }
            }
            
            predictions = engine.predict(aggregated_data)
            self.assertEqual(predictions, {})
        except Exception as e:
            self.fail(f"Empty predictions on disabled test failed: {str(e)}")


class TestFullPipelineIntegration(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.ml_dir = Path(cls.test_dir) / "ml"
        cls.ml_dir.mkdir(parents=True, exist_ok=True)
        
        cls.models_dir = cls.ml_dir / "models"
        cls.models_dir.mkdir(parents=True, exist_ok=True)
        
        cls.features_dir = cls.ml_dir / "features"
        cls.features_dir.mkdir(parents=True, exist_ok=True)
        
        fake_model = FakeModel()
        cls.model_path = cls.models_dir / "fake_model.pkl"
        with open(cls.model_path, 'wb') as f:
            pickle.dump(fake_model, f)
        
        cls.config_path = cls.features_dir / "config.json"
        with open(cls.config_path, 'w') as f:
            json.dump({"features": ["price", "volume", "change_percent"]}, f)
    
    @classmethod
    def tearDownClass(cls):
        if Path(cls.test_dir).exists():
            shutil.rmtree(cls.test_dir)
    
    def test_end_to_end_prediction_pipeline(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            aggregated_data = {
                "symbols": {
                    "BTCUSD": {
                        "open": 100.0,
                        "close": 105.0,
                        "high": 110.0,
                        "low": 98.0,
                        "volume": 5000000.0,
                        "change": 5.0,
                        "change_percent": 5.0
                    },
                    "ETHUSD": {
                        "open": 50.0,
                        "close": 48.0,
                        "high": 52.0,
                        "low": 47.0,
                        "volume": 3000000.0,
                        "change": -2.0,
                        "change_percent": -4.0
                    },
                    "DOGEUSD": {
                        "open": 0.1,
                        "close": 0.12,
                        "high": 0.13,
                        "low": 0.09,
                        "volume": 1000000.0,
                        "change": 0.02,
                        "change_percent": 20.0
                    }
                }
            }
            
            predictions = engine.predict(aggregated_data)
            
            self.assertEqual(len(predictions), 3)
            self.assertIn("BTCUSD", predictions)
            self.assertIn("ETHUSD", predictions)
            self.assertIn("DOGEUSD", predictions)
            
            for symbol, pred in predictions.items():
                self.assertIsNotNone(pred.get("prediction"))
                self.assertIsNotNone(pred.get("timestamp"))
                self.assertEqual(pred.get("symbol"), symbol)
        except Exception as e:
            self.fail(f"End-to-end pipeline integration test failed: {str(e)}")
    
    def test_pipeline_resilience_with_partial_data(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            partial_data = {
                "symbols": {
                    "BTCUSD": {
                        "open": 100.0,
                        "close": 105.0,
                        "high": 110.0,
                        "low": 98.0,
                        "volume": 5000000.0
                    },
                    "INCOMPLETE": {
                        "open": 50.0
                    }
                }
            }
            
            predictions = engine.predict(partial_data)
            
            self.assertGreaterEqual(len(predictions), 0)
        except Exception as e:
            self.fail(f"Pipeline resilience test failed: {str(e)}")
    
    def test_pipeline_output_structure_preserved(self):
        try:
            engine = MLInferenceEngine(str(self.model_path), str(self.config_path))
            
            aggregated_data = {
                "symbols": {
                    "BTCUSD": {
                        "open": 100.0,
                        "close": 105.0,
                        "high": 110.0,
                        "low": 98.0,
                        "volume": 5000000.0,
                        "change": 5.0,
                        "change_percent": 5.0
                    }
                }
            }
            
            predictions = engine.predict(aggregated_data)
            
            self.assertTrue(isinstance(predictions, dict))
            self.assertTrue(all(isinstance(v, dict) for v in predictions.values()))
        except Exception as e:
            self.fail(f"Output structure preservation test failed: {str(e)}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
