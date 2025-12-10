import pickle
import json
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import traceback

from src.utils.logger import get_logger


class MLInferenceEngine:
    def __init__(self, model_path: str, feature_config_path: str):
        self.logger = get_logger("MLInferenceEngine")
        self.model = None
        self.feature_config = None
        self.feature_columns = []
        
        try:
            self._load_model(model_path)
            self._load_feature_config(feature_config_path)
            self.logger.info("MLInferenceEngine initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize MLInferenceEngine: {str(e)}")
            raise
    
    def _load_model(self, model_path: str) -> None:
        try:
            path = Path(model_path)
            if not path.exists():
                raise FileNotFoundError(f"Model file not found: {model_path}")
            
            with open(path, 'rb') as f:
                self.model = pickle.load(f)
            
            self.logger.info(f"Model loaded successfully: {model_path}")
        except Exception as e:
            self.logger.error(f"Failed to load model: {str(e)}")
            raise
    
    def _load_feature_config(self, feature_config_path: str) -> None:
        try:
            path = Path(feature_config_path)
            if not path.exists():
                raise FileNotFoundError(f"Feature config not found: {feature_config_path}")
            
            with open(path, 'r', encoding='utf-8') as f:
                self.feature_config = json.load(f)
            
            if not self.feature_config:
                raise ValueError("Feature config is empty")
            
            self.feature_columns = self.feature_config.get("features", [])
            if not self.feature_columns:
                self.logger.warning("No features configured in feature config")
            
            self.logger.info(f"Feature config loaded: {len(self.feature_columns)} features")
        except Exception as e:
            self.logger.error(f"Failed to load feature config: {str(e)}")
            raise
    
    def _extract_features(self, symbol_data: Dict[str, Any]) -> Optional[Dict[str, float]]:
        try:
            if not symbol_data or not isinstance(symbol_data, dict):
                return None
            
            features = {}
            
            price = symbol_data.get("price", 0.0)
            open_price = symbol_data.get("open", 0.0)
            high = symbol_data.get("high", 0.0)
            low = symbol_data.get("low", 0.0)
            volume = symbol_data.get("volume", 0.0)
            change = symbol_data.get("change", 0.0)
            change_percent = symbol_data.get("change_percent", 0.0)
            
            features["price"] = float(price) if price else 0.0
            features["open"] = float(open_price) if open_price else 0.0
            features["high"] = float(high) if high else 0.0
            features["low"] = float(low) if low else 0.0
            features["volume"] = float(volume) if volume else 0.0
            features["change"] = float(change) if change else 0.0
            features["change_percent"] = float(change_percent) if change_percent else 0.0
            
            if high > 0 and low > 0:
                features["high_low_ratio"] = high / low if low != 0 else 1.0
            else:
                features["high_low_ratio"] = 1.0
            
            if open_price > 0:
                features["price_change_ratio"] = price / open_price if open_price != 0 else 1.0
            else:
                features["price_change_ratio"] = 1.0
            
            klines = symbol_data.get("klines", [])
            features["klines_count"] = float(len(klines)) if klines else 0.0
            
            return features
        except Exception as e:
            self.logger.warning(f"Feature extraction failed: {str(e)}")
            return None
    
    def _prepare_prediction_input(self, features: Dict[str, float]) -> Optional[np.ndarray]:
        try:
            if not features or not self.feature_columns:
                return None
            
            feature_vector = []
            for col in self.feature_columns:
                value = features.get(col, 0.0)
                feature_vector.append(float(value) if value is not None else 0.0)
            
            if not feature_vector:
                return None
            
            return np.array([feature_vector])
        except Exception as e:
            self.logger.warning(f"Failed to prepare prediction input: {str(e)}")
            return None
    
    def _safe_predict(self, input_array: np.ndarray) -> Optional[Dict[str, Any]]:
        try:
            if not self.model or input_array is None:
                return None
            
            prediction = self.model.predict(input_array)
            
            prediction_dict = {
                "prediction": float(prediction[0]) if hasattr(prediction, '__getitem__') else float(prediction),
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "confidence": 0.85
            }
            
            if hasattr(self.model, 'predict_proba'):
                try:
                    probabilities = self.model.predict_proba(input_array)
                    prediction_dict["probabilities"] = [float(p) for p in probabilities[0]]
                except Exception:
                    pass
            
            return prediction_dict
        except Exception as e:
            self.logger.warning(f"Model prediction failed: {str(e)}")
            return None
    
    def predict_single(self, symbol: str, symbol_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            if not symbol or not symbol_data:
                return None
            
            features = self._extract_features(symbol_data)
            if not features:
                return None
            
            input_array = self._prepare_prediction_input(features)
            if input_array is None:
                return None
            
            prediction = self._safe_predict(input_array)
            if not prediction:
                return None
            
            prediction["symbol"] = symbol
            prediction["features_used"] = len(self.feature_columns)
            
            return prediction
        except Exception as e:
            self.logger.warning(f"Single symbol prediction failed for {symbol}: {str(e)}")
            return None
    
    def predict(self, aggregated_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        try:
            if not aggregated_data or "symbols" not in aggregated_data:
                return {}
            
            predictions = {}
            symbols_data = aggregated_data.get("symbols", {})
            
            if not symbols_data:
                return {}
            
            for symbol, symbol_data in symbols_data.items():
                try:
                    prediction = self.predict_single(symbol, symbol_data)
                    if prediction:
                        predictions[symbol] = prediction
                except Exception as e:
                    self.logger.warning(f"Failed to predict for {symbol}: {str(e)}")
                    continue
            
            if predictions:
                self.logger.info(f"Generated predictions for {len(predictions)} symbols")
            else:
                self.logger.warning("No predictions generated")
            
            return predictions
        except Exception as e:
            self.logger.error(f"Batch prediction failed: {str(e)}\n{traceback.format_exc()}")
            return {}
