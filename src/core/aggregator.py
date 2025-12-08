from src.utils.logger import get_logger, log_metric, correlation_decorator
from src.utils.config_loader import ConfigLoader
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import os
import json
import numpy as np
from enum import Enum
import threading
import traceback


class DataQuality(Enum):
    VALID = 1
    PARTIAL = 0.5
    INVALID = 0


class DataAggregator:
    
    def __init__(self, config_path: str = "config/settings.json"):
        self.logger = get_logger("DataAggregator")
        self.config = None
        self.lock = threading.RLock()
        self.normalized_cache = {}
        self.feature_cache = {}
        
        try:
            self._load_config(config_path)
            self._validate_paths()
            self.logger.info("DataAggregator initialized (4-Layer Architecture)")
            log_metric(
                "aggregator_init",
                1,
                {"status": "success", "layers": 4}
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize DataAggregator: {str(e)}")
            log_metric("aggregator_init", 0, {"status": "failed", "error": str(e)})
            raise
    
    def _load_config(self, config_path: str) -> None:
        try:
            self.config = ConfigLoader.load(config_path)
            if not self.config:
                raise ValueError("Configuration is empty")
        except Exception as e:
            self.logger.error(f"Config loading failed: {str(e)}")
            raise
    
    def _validate_paths(self) -> None:
        try:
            paths = {
                "processed": self.config.get("data_sources", {}).get("processed_data_path", "data/processed/"),
                "exports": self.config.get("export", {}).get("excel_output_folder", "data/exports/"),
                "ml_ready": "data/ml_ready/"
            }
            
            for path_name, path_str in paths.items():
                path = Path(path_str)
                if not path.exists():
                    path.mkdir(parents=True, exist_ok=True)
                    self.logger.info(f"Created {path_name} directory: {path_str}")
        except Exception as e:
            self.logger.error(f"Path validation failed: {str(e)}")
            raise
    
    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default
    
    def _is_crypto(self, symbol: str) -> bool:
        if not symbol or not isinstance(symbol, str):
            return False
        
        symbol_upper = symbol.upper().strip()
        crypto_indicators = ["BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "AVAX", 
                            "MATIC", "DOT", "LINK", "UNI", "LTC", "BCH", "NEAR", "USDT", 
                            "USD", "BUSD"]
        
        return any(indicator in symbol_upper for indicator in crypto_indicators)
    
    def normalize_single(self, raw_item: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if not isinstance(raw_item, dict) or not symbol:
                self.logger.warning(f"Invalid input for normalize_single: symbol={symbol}")
                return None
            
            symbol = symbol.upper().strip()
            asset_type = "crypto" if self._is_crypto(symbol) else "stock"
            
            price = self._safe_float(raw_item.get("price") or raw_item.get("c"))
            open_price = self._safe_float(raw_item.get("open") or raw_item.get("o"))
            high = self._safe_float(raw_item.get("high") or raw_item.get("h"))
            low = self._safe_float(raw_item.get("low") or raw_item.get("l"))
            volume = self._safe_float(raw_item.get("volume") or raw_item.get("v"))
            
            change = self._safe_float(raw_item.get("change", 0))
            if change == 0 and open_price > 0:
                change = price - open_price
            
            change_percent = self._safe_float(raw_item.get("change_percent", 0))
            if change_percent == 0 and open_price > 0:
                change_percent = (change / open_price) * 100
            
            timestamp_raw = raw_item.get("timestamp")
            if isinstance(timestamp_raw, str):
                timestamp = timestamp_raw
            else:
                timestamp = datetime.utcnow().isoformat() + "Z"
            
            normalized = {
                "symbol": symbol,
                "type": asset_type,
                "price": price,
                "open": open_price,
                "high": high,
                "low": low,
                "volume": volume,
                "change": change,
                "change_percent": change_percent,
                "timestamp": timestamp,
                "api": raw_item.get("api", "unknown"),
                "klines": raw_item.get("klines", []),
                "klines_count": len(raw_item.get("klines", []))
            }
            
            return normalized
        
        except Exception as e:
            self.logger.error(f"Error in normalize_single for {symbol}: {str(e)}\n{traceback.format_exc()}")
            return None
    
    def normalize_all(self, raw_dict: Dict[str, Any]) -> Dict[str, Any]:
        try:
            normalized_data = {}
            
            if not isinstance(raw_dict, dict):
                self.logger.warning("normalize_all received non-dict input")
                return {}
            
            for symbol, raw_item in raw_dict.items():
                try:
                    normalized = self.normalize_single(raw_item, symbol)
                    if normalized:
                        normalized_data[symbol] = normalized
                    else:
                        self.logger.warning(f"Failed to normalize {symbol}")
                except Exception as e:
                    self.logger.error(f"Exception normalizing {symbol}: {str(e)}")
                    continue
            
            self.logger.info(f"Normalized {len(normalized_data)}/{len(raw_dict)} items")
            log_metric("normalize_all", len(normalized_data), {"total": len(raw_dict)})
            
            with self.lock:
                self.normalized_cache = normalized_data
            
            return normalized_data
        
        except Exception as e:
            self.logger.error(f"Error in normalize_all: {str(e)}\n{traceback.format_exc()}")
            return {}
    
    def compute_basic_features(self, item: Dict[str, Any]) -> Dict[str, Any]:
        try:
            features = {}
            
            price = item.get("price", 0)
            open_price = item.get("open", 0)
            high = item.get("high", 0)
            low = item.get("low", 0)
            volume = item.get("volume", 0)
            
            features["price_diff"] = self._safe_float(price - open_price)
            
            features["pct_change"] = self._safe_float(
                (features["price_diff"] / open_price * 100) if open_price > 0 else 0
            )
            
            price_range = self._safe_float(high - low)
            features["price_range"] = price_range
            
            features["volatility"] = self._safe_float(
                (price_range / price) * 100 if price > 0 else 0
            )
            
            features["hl_ratio"] = self._safe_float(
                high / low if low > 0 else 1.0
            )
            
            features["price_position"] = self._safe_float(
                ((price - low) / price_range * 100) if price_range > 0 else 50
            )
            
            return features
        
        except Exception as e:
            self.logger.error(f"Error in compute_basic_features: {str(e)}")
            return {}
    
    def compute_technical_indicators(self, item: Dict[str, Any]) -> Dict[str, Any]:
        try:
            indicators = {}
            
            price = self._safe_float(item.get("price", 0))
            volume = self._safe_float(item.get("volume", 0))
            high = self._safe_float(item.get("high", 0))
            low = self._safe_float(item.get("low", 0))
            
            indicators["rsi_14"] = self._compute_rsi_simple(price)
            
            indicators["ema_12"] = self._compute_ema_simple(price, 12)
            indicators["ema_26"] = self._compute_ema_simple(price, 26)
            
            indicators["macd"] = self._safe_float(indicators["ema_12"] - indicators["ema_26"])
            indicators["macd_signal"] = self._safe_float(indicators["macd"] * 0.9)
            
            indicators["atr"] = self._compute_atr(high, low, price)
            
            indicators["vwap"] = self._compute_vwap(price, volume)
            
            return indicators
        
        except Exception as e:
            self.logger.error(f"Error in compute_technical_indicators: {str(e)}")
            return {}
    
    def _compute_rsi_simple(self, price: float, period: int = 14) -> float:
        try:
            if price <= 0:
                return 50.0
            
            gain = max(0, price * 0.01)
            loss = max(0, -price * 0.01)
            
            avg_gain = gain if gain > 0 else 0.01
            avg_loss = loss if loss > 0 else 0.01
            
            rs = avg_gain / avg_loss if avg_loss > 0 else 1.0
            rsi = 100 - (100 / (1 + rs))
            
            return self._safe_float(max(0, min(100, rsi)))
        except Exception as e:
            self.logger.debug(f"RSI computation failed: {str(e)}")
            return 50.0
    
    def _compute_ema_simple(self, price: float, period: int) -> float:
        try:
            if price <= 0:
                return 0.0
            
            multiplier = 2.0 / (period + 1)
            ema = price * multiplier + price * (1 - multiplier)
            
            return self._safe_float(ema)
        except Exception as e:
            self.logger.debug(f"EMA computation failed: {str(e)}")
            return price
    
    def _compute_atr(self, high: float, low: float, close: float) -> float:
        try:
            if high <= 0 or low <= 0:
                return 0.0
            
            tr = max(high - low, abs(high - close), abs(low - close))
            atr = tr
            
            return self._safe_float(atr)
        except Exception as e:
            self.logger.debug(f"ATR computation failed: {str(e)}")
            return 0.0
    
    def _compute_vwap(self, price: float, volume: float) -> float:
        try:
            if volume <= 0:
                return price
            
            vwap = price
            return self._safe_float(vwap)
        except Exception as e:
            self.logger.debug(f"VWAP computation failed: {str(e)}")
            return price
    
    def compute_kline_features(self, item: Dict[str, Any]) -> Dict[str, Any]:
        try:
            features = {}
            klines = item.get("klines", [])
            
            if not klines or not isinstance(klines, list):
                return {
                    "rolling_mean_24h": 0.0,
                    "rolling_std_24h": 0.0,
                    "rolling_volume_sum_24h": 0.0,
                    "rolling_volume_change": 0.0,
                    "kline_count": 0
                }
            
            try:
                prices = []
                volumes = []
                
                for kline in klines[-24:]:
                    if isinstance(kline, (list, tuple)) and len(kline) >= 5:
                        try:
                            close_price = self._safe_float(kline[4])
                            kline_volume = self._safe_float(kline[7] if len(kline) > 7 else 0)
                            prices.append(close_price)
                            volumes.append(kline_volume)
                        except (IndexError, ValueError):
                            continue
                
                if prices:
                    features["rolling_mean_24h"] = self._safe_float(np.mean(prices))
                    features["rolling_std_24h"] = self._safe_float(np.std(prices))
                else:
                    features["rolling_mean_24h"] = 0.0
                    features["rolling_std_24h"] = 0.0
                
                features["rolling_volume_sum_24h"] = self._safe_float(np.sum(volumes))
                
                if len(volumes) >= 2:
                    features["rolling_volume_change"] = self._safe_float(
                        ((volumes[-1] - volumes[0]) / volumes[0] * 100) if volumes[0] > 0 else 0
                    )
                else:
                    features["rolling_volume_change"] = 0.0
                
                features["kline_count"] = len(klines)
                
            except Exception as e:
                self.logger.debug(f"Kline processing error: {str(e)}")
                features["rolling_mean_24h"] = 0.0
                features["rolling_std_24h"] = 0.0
                features["rolling_volume_sum_24h"] = 0.0
                features["rolling_volume_change"] = 0.0
                features["kline_count"] = len(klines)
            
            return features
        
        except Exception as e:
            self.logger.error(f"Error in compute_kline_features: {str(e)}")
            return {
                "rolling_mean_24h": 0.0,
                "rolling_std_24h": 0.0,
                "rolling_volume_sum_24h": 0.0,
                "rolling_volume_change": 0.0,
                "kline_count": 0
            }
    
    def compute_ml_features(self, item: Dict[str, Any]) -> Dict[str, Any]:
        try:
            ml_features = {}
            
            price = self._safe_float(item.get("price", 0))
            volume = self._safe_float(item.get("volume", 0))
            high = self._safe_float(item.get("high", 0))
            low = self._safe_float(item.get("low", 0))
            
            change_percent = self._safe_float(item.get("change_percent", 0))
            ml_features["momentum"] = self._safe_float(change_percent)
            
            volatility = self._safe_float((high - low) / price * 100 if price > 0 else 0)
            ml_features["trend_strength"] = self._safe_float(
                abs(change_percent) / (volatility + 0.001)
            )
            
            ml_features["volatility_normalized"] = self._safe_float(
                min(100, max(0, volatility))
            )
            
            ml_features["volume_normalized"] = self._safe_float(
                min(1000000, volume) / 1000000 * 100 if volume > 0 else 0
            )
            
            ml_features["price_position_ratio"] = self._safe_float(
                ((price - low) / (high - low)) if (high - low) > 0 else 0.5
            )
            
            ml_features["volume_price_ratio"] = self._safe_float(
                (volume / price * 100) if price > 0 else 0
            )
            
            quality = 1 if all([price > 0, high >= low, volume >= 0]) else 0
            ml_features["data_quality_score"] = float(quality)
            
            return ml_features
        
        except Exception as e:
            self.logger.error(f"Error in compute_ml_features: {str(e)}")
            return {
                "momentum": 0.0,
                "trend_strength": 0.0,
                "volatility_normalized": 0.0,
                "volume_normalized": 0.0,
                "price_position_ratio": 0.5,
                "volume_price_ratio": 0.0,
                "data_quality_score": 0.0
            }
    
    def compute_market_wide_features(self, all_items: Dict[str, Any]) -> Dict[str, Any]:
        try:
            market_summary = {}
            
            if not all_items:
                return self._get_empty_market_summary()
            
            crypto_changes = []
            stock_changes = []
            all_volatilities = []
            all_volumes = []
            
            for symbol, item in all_items.items():
                try:
                    change_pct = self._safe_float(item.get("change_percent", 0))
                    volatility = self._safe_float((item.get("high", 0) - item.get("low", 0)) / item.get("price", 1) * 100 if item.get("price", 0) > 0 else 0)
                    volume = self._safe_float(item.get("volume", 0))
                    
                    all_volatilities.append(volatility)
                    all_volumes.append(volume)
                    
                    if item.get("type") == "crypto":
                        crypto_changes.append(change_pct)
                    else:
                        stock_changes.append(change_pct)
                except Exception as e:
                    self.logger.debug(f"Market feature calculation error for {symbol}: {str(e)}")
                    continue
            
            market_summary["crypto_strength"] = self._safe_float(
                np.mean(crypto_changes) if crypto_changes else 0
            )
            
            market_summary["stock_strength"] = self._safe_float(
                np.mean(stock_changes) if stock_changes else 0
            )
            
            market_summary["sector_divergence"] = self._safe_float(
                abs(market_summary["crypto_strength"] - market_summary["stock_strength"])
            )
            
            market_summary["global_volatility"] = self._safe_float(
                np.mean(all_volatilities) if all_volatilities else 0
            )
            
            market_summary["total_volume"] = self._safe_float(np.sum(all_volumes))
            market_summary["asset_count"] = len(all_items)
            market_summary["timestamp"] = datetime.utcnow().isoformat() + "Z"
            
            return market_summary
        
        except Exception as e:
            self.logger.error(f"Error in compute_market_wide_features: {str(e)}\n{traceback.format_exc()}")
            return self._get_empty_market_summary()
    
    def _get_empty_market_summary(self) -> Dict[str, Any]:
        return {
            "crypto_strength": 0.0,
            "stock_strength": 0.0,
            "sector_divergence": 0.0,
            "global_volatility": 0.0,
            "total_volume": 0.0,
            "asset_count": 0,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    
    def compute_correlations(self, all_items: Dict[str, Any]) -> Dict[str, Any]:
        try:
            correlations = {}
            
            if not all_items or len(all_items) < 2:
                return {"status": "insufficient_data"}
            
            crypto_items = {k: v for k, v in all_items.items() if v.get("type") == "crypto"}
            stock_items = {k: v for k, v in all_items.items() if v.get("type") == "stock"}
            
            if len(crypto_items) >= 2:
                btc_data = next((v for k, v in crypto_items.items() if "BTC" in k.upper()), None)
                eth_data = next((v for k, v in crypto_items.items() if "ETH" in k.upper()), None)
                
                if btc_data and eth_data:
                    btc_change = self._safe_float(btc_data.get("change_percent", 0))
                    eth_change = self._safe_float(eth_data.get("change_percent", 0))
                    
                    btc_vol = self._safe_float(btc_data.get("volatility", 0))
                    eth_vol = self._safe_float(eth_data.get("volatility", 0))
                    
                    if btc_vol > 0 and eth_vol > 0:
                        correlation = self._safe_float(
                            (btc_change * eth_change) / (btc_vol * eth_vol + 0.001)
                        )
                        correlations["BTC_ETH"] = min(1.0, max(-1.0, correlation))
                    else:
                        correlations["BTC_ETH"] = 0.0
            
            if len(stock_items) >= 2:
                stock_changes = [self._safe_float(v.get("change_percent", 0)) for v in stock_items.values()]
                if stock_changes:
                    correlations["stock_avg_correlation"] = self._safe_float(np.mean(stock_changes))
                    correlations["stock_volatility"] = self._safe_float(np.std(stock_changes))
            
            if crypto_items and stock_items:
                crypto_avg_change = np.mean([self._safe_float(v.get("change_percent", 0)) for v in crypto_items.values()])
                stock_avg_change = np.mean([self._safe_float(v.get("change_percent", 0)) for v in stock_items.values()])
                correlations["crypto_vs_stock"] = self._safe_float(crypto_avg_change - stock_avg_change)
            
            correlations["timestamp"] = datetime.utcnow().isoformat() + "Z"
            
            return correlations
        
        except Exception as e:
            self.logger.error(f"Error in compute_correlations: {str(e)}\n{traceback.format_exc()}")
            return {"status": "error", "timestamp": datetime.utcnow().isoformat() + "Z"}
    
    def aggregate(self, raw_dict: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not isinstance(raw_dict, dict) or not raw_dict:
                self.logger.warning("aggregate received empty or invalid input")
                return self._get_empty_aggregate()
            
            normalized = self.normalize_all(raw_dict)
            
            aggregated_symbols = {}
            for symbol, norm_item in normalized.items():
                try:
                    symbol_data = {
                        "normalized": norm_item,
                        "basic_features": self.compute_basic_features(norm_item),
                        "technical_indicators": self.compute_technical_indicators(norm_item),
                        "ml_features": self.compute_ml_features(norm_item)
                    }
                    
                    if norm_item.get("type") == "crypto" and norm_item.get("klines"):
                        symbol_data["kline_features"] = self.compute_kline_features(norm_item)
                    
                    aggregated_symbols[symbol] = symbol_data
                except Exception as e:
                    self.logger.error(f"Error aggregating {symbol}: {str(e)}")
                    continue
            
            market_summary = self.compute_market_wide_features(normalized)
            correlations = self.compute_correlations(normalized)
            
            final_aggregated = {
                "symbols": aggregated_symbols,
                "market_summary": market_summary,
                "correlations": correlations,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metadata": {
                    "total_symbols": len(aggregated_symbols),
                    "crypto_count": len([s for s in aggregated_symbols.values() if s["normalized"].get("type") == "crypto"]),
                    "stock_count": len([s for s in aggregated_symbols.values() if s["normalized"].get("type") == "stock"])
                }
            }
            
            with self.lock:
                self.feature_cache = final_aggregated
            
            self.logger.info(f"Aggregated {len(aggregated_symbols)} symbols successfully")
            log_metric("aggregation_complete", len(aggregated_symbols), {"status": "success"})
            
            return final_aggregated
        
        except Exception as e:
            self.logger.error(f"Error in aggregate: {str(e)}\n{traceback.format_exc()}")
            log_metric("aggregation_complete", 0, {"status": "failed", "error": str(e)})
            return self._get_empty_aggregate()
    
    def _get_empty_aggregate(self) -> Dict[str, Any]:
        return {
            "symbols": {},
            "market_summary": self._get_empty_market_summary(),
            "correlations": {"status": "no_data"},
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metadata": {
                "total_symbols": 0,
                "crypto_count": 0,
                "stock_count": 0
            }
        }
    
    def save_excel_ready(self, aggregated: Dict[str, Any]) -> bool:
        try:
            if not aggregated or not aggregated.get("symbols"):
                self.logger.warning("save_excel_ready: no data to save")
                return False
            
            excel_rows = []
            symbols_data = aggregated.get("symbols", {})
            
            for symbol, data in symbols_data.items():
                try:
                    norm = data.get("normalized", {})
                    basic = data.get("basic_features", {})
                    tech = data.get("technical_indicators", {})
                    ml = data.get("ml_features", {})
                    kline = data.get("kline_features", {})
                    
                    row = {
                        "symbol": symbol,
                        "type": norm.get("type", "unknown"),
                        "price": norm.get("price", 0),
                        "change": norm.get("change", 0),
                        "change_percent": norm.get("change_percent", 0),
                        "volume": norm.get("volume", 0),
                        "high": norm.get("high", 0),
                        "low": norm.get("low", 0),
                        "volatility": basic.get("volatility", 0),
                        "rsi_14": tech.get("rsi_14", 0),
                        "macd": tech.get("macd", 0),
                        "atr": tech.get("atr", 0),
                        "momentum": ml.get("momentum", 0),
                        "trend_strength": ml.get("trend_strength", 0),
                        "data_quality": ml.get("data_quality_score", 0),
                        "timestamp": norm.get("timestamp", datetime.utcnow().isoformat())
                    }
                    
                    if kline:
                        row.update({
                            "rolling_mean_24h": kline.get("rolling_mean_24h", 0),
                            "rolling_std_24h": kline.get("rolling_std_24h", 0),
                            "kline_count": kline.get("kline_count", 0)
                        })
                    
                    excel_rows.append(row)
                except Exception as e:
                    self.logger.error(f"Error preparing row for {symbol}: {str(e)}")
                    continue
            
            if not excel_rows:
                self.logger.warning("save_excel_ready: no rows to save")
                return False
            
            output_dir = self.config.get("export", {}).get("excel_output_folder", "data/exports/")
            prefix = self.config.get("export", {}).get("excel_file_prefix", "market_data_")
            include_timestamp = self.config.get("export", {}).get("include_timestamp_in_filename", True)
            
            if include_timestamp:
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                filename = f"{prefix}{timestamp}.json"
            else:
                filename = f"{prefix}latest.json"
            
            filepath = Path(output_dir) / filename
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(excel_rows, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved Excel-ready data: {filepath} ({len(excel_rows)} rows)")
            log_metric("excel_ready_save", len(excel_rows), {"filepath": str(filepath)})
            
            return True
        
        except Exception as e:
            self.logger.error(f"Error in save_excel_ready: {str(e)}\n{traceback.format_exc()}")
            log_metric("excel_ready_save", 0, {"status": "failed", "error": str(e)})
            return False
    
    def save_ml_ready(self, aggregated: Dict[str, Any]) -> bool:
        try:
            if not aggregated or not aggregated.get("symbols"):
                self.logger.warning("save_ml_ready: no data to save")
                return False
            
            ml_ready_data = {}
            symbols_data = aggregated.get("symbols", {})
            
            for symbol, data in symbols_data.items():
                try:
                    norm = data.get("normalized", {})
                    ml = data.get("ml_features", {})
                    tech = data.get("technical_indicators", {})
                    basic = data.get("basic_features", {})
                    kline = data.get("kline_features", {})
                    
                    ml_ready_data[symbol] = {
                        "features": {
                            "price": norm.get("price", 0),
                            "volume": norm.get("volume", 0),
                            "momentum": ml.get("momentum", 0),
                            "trend_strength": ml.get("trend_strength", 0),
                            "volatility_normalized": ml.get("volatility_normalized", 0),
                            "volume_normalized": ml.get("volume_normalized", 0),
                            "price_position_ratio": ml.get("price_position_ratio", 0.5),
                            "rsi_14": tech.get("rsi_14", 0),
                            "macd": tech.get("macd", 0),
                            "atr": tech.get("atr", 0),
                            "ema_12": tech.get("ema_12", 0),
                            "ema_26": tech.get("ema_26", 0),
                            "price_volatility": basic.get("volatility", 0)
                        },
                        "metadata": {
                            "type": norm.get("type", "unknown"),
                            "api": norm.get("api", "unknown"),
                            "data_quality": ml.get("data_quality_score", 0),
                            "timestamp": norm.get("timestamp", datetime.utcnow().isoformat())
                        }
                    }
                    
                    if kline:
                        ml_ready_data[symbol]["kline_features"] = {
                            "rolling_mean_24h": kline.get("rolling_mean_24h", 0),
                            "rolling_std_24h": kline.get("rolling_std_24h", 0),
                            "rolling_volume_sum_24h": kline.get("rolling_volume_sum_24h", 0),
                            "rolling_volume_change": kline.get("rolling_volume_change", 0),
                            "kline_count": kline.get("kline_count", 0)
                        }
                
                except Exception as e:
                    self.logger.error(f"Error preparing ML data for {symbol}: {str(e)}")
                    continue
            
            if not ml_ready_data:
                self.logger.warning("save_ml_ready: no data prepared")
                return False
            
            market_summary = aggregated.get("market_summary", {})
            correlations = aggregated.get("correlations", {})
            
            full_ml_data = {
                "symbols": ml_ready_data,
                "market_summary": market_summary,
                "correlations": correlations,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
            
            output_dir = "data/ml_ready/"
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"ml_data_{timestamp}.json"
            filepath = Path(output_dir) / filename
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(full_ml_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved ML-ready data: {filepath} ({len(ml_ready_data)} symbols)")
            log_metric("ml_ready_save", len(ml_ready_data), {"filepath": str(filepath)})
            
            return True
        
        except Exception as e:
            self.logger.error(f"Error in save_ml_ready: {str(e)}\n{traceback.format_exc()}")
            log_metric("ml_ready_save", 0, {"status": "failed", "error": str(e)})
            return False
    
    @correlation_decorator()
    def run(self, raw_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            self.logger.info(f"Starting aggregation for {len(raw_dict)} symbols")
            
            aggregated = self.aggregate(raw_dict)
            
            if aggregated.get("metadata", {}).get("total_symbols", 0) > 0:
                excel_saved = self.save_excel_ready(aggregated)
                ml_saved = self.save_ml_ready(aggregated)
                
                log_metric("run_complete", 1, {
                    "symbols": len(aggregated.get("symbols", {})),
                    "excel_saved": excel_saved,
                    "ml_saved": ml_saved
                })
                
                return aggregated
            else:
                self.logger.warning("Aggregation produced no valid symbols")
                log_metric("run_complete", 0, {"status": "no_valid_symbols"})
                return aggregated
        
        except Exception as e:
            self.logger.error(f"Error in run: {str(e)}\n{traceback.format_exc()}")
            log_metric("run_complete", 0, {"status": "failed", "error": str(e)})
            return None
    
    def get_cache_status(self) -> Dict[str, Any]:
        try:
            with self.lock:
                return {
                    "normalized_cache_size": len(self.normalized_cache),
                    "feature_cache_size": len(self.feature_cache),
                    "timestamp": datetime.utcnow().isoformat()
                }
        except Exception as e:
            self.logger.error(f"Error in get_cache_status: {str(e)}")
            return {"status": "error"}
    
    def clear_cache(self) -> bool:
        try:
            with self.lock:
                self.normalized_cache.clear()
                self.feature_cache.clear()
            self.logger.info("Cache cleared successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error clearing cache: {str(e)}")
            return False
