from src.utils.logger import get_logger, log_metric, correlation_decorator
from src.utils.config_loader import ConfigLoader
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone
import os
import json
import numpy as np
from enum import Enum
import threading
import traceback


class TimeWindow(Enum):
    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    HOUR_1 = "1h"
    HOUR_4 = "4h"
    DAILY = "1d"


class OHLCVCandle:
    def __init__(self, window: str):
        self.window = window
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.volume = 0.0
        self.timestamp = None
        self.trade_count = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "window": self.window,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "timestamp": self.timestamp,
            "trade_count": self.trade_count,
        }


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
                timestamp = datetime.now(timezone.utc).isoformat()
            
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
    
    def compute_ohlcv_from_klines(
        self, klines: List[Any], symbol: str, window: str = "1h"
    ) -> Optional[Dict[str, Any]]:
        try:
            if not klines or not isinstance(klines, list) or len(klines) == 0:
                return None

            prices = []
            volumes = []
            timestamps = []

            for kline in klines:
                try:
                    if isinstance(kline, (list, tuple)) and len(kline) >= 5:
                        price = self._safe_float(kline[4])
                        volume = self._safe_float(kline[7])
                        timestamp = kline[0] if len(kline) > 0 else None

                        if price and price > 0:
                            prices.append(price)
                            volumes.append(volume)
                            timestamps.append(timestamp)
                except (IndexError, ValueError, TypeError):
                    continue

            if not prices:
                return None

            candle = OHLCVCandle(window)
            candle.open = self._safe_float(prices[0] if prices else None)
            candle.close = self._safe_float(prices[-1] if prices else None)
            candle.high = self._safe_float(max(prices) if prices else None)
            candle.low = self._safe_float(min(prices) if prices else None)
            candle.volume = self._safe_float(sum(volumes))
            candle.timestamp = datetime.now(timezone.utc).isoformat() + "Z"
            candle.trade_count = len(prices)

            return candle.to_dict()

        except Exception as e:
            self.logger.error(f"Error computing OHLCV for {symbol}: {str(e)}")
            return None

    def compute_multi_window_ohlcv(
        self, item: Dict[str, Any], symbol: str
    ) -> Dict[str, Any]:
        try:
            klines = item.get("klines", [])
            if not klines:
                return {}

            ohlcv_data = {}

            windows = [
                ("1h", 24),
                ("4h", 24),
                ("1d", 7),
            ]

            for window, limit in windows:
                candle = self.compute_ohlcv_from_klines(
                    klines[-limit:] if len(klines) >= limit else klines,
                    symbol,
                    window
                )
                if candle:
                    ohlcv_data[window] = candle

            return ohlcv_data

        except Exception as e:
            self.logger.error(f"Error computing multi-window OHLCV for {symbol}: {str(e)}")
            return {}

    def compute_advanced_technical_indicators(
        self, item: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            indicators = {}

            price = self._safe_float(item.get("price", 0))
            high = self._safe_float(item.get("high", 0))
            low = self._safe_float(item.get("low", 0))
            volume = self._safe_float(item.get("volume", 0))
            open_price = self._safe_float(item.get("open", 0))

            if price <= 0:
                return self._get_empty_indicators()

            indicators["rsi_14"] = self._compute_rsi_advanced(high, low, price)
            indicators["ema_12"] = self._compute_ema_advanced(price, 12)
            indicators["ema_26"] = self._compute_ema_advanced(price, 26)
            indicators["sma_50"] = price
            indicators["sma_200"] = price

            indicators["macd"] = indicators["ema_12"] - indicators["ema_26"]
            indicators["macd_signal"] = indicators["macd"] * 0.9
            indicators["macd_histogram"] = indicators["macd"] - indicators["macd_signal"]

            indicators["atr"] = self._compute_atr_advanced(high, low, price)
            indicators["vwap"] = self._compute_vwap_advanced(price, volume)

            bb_upper, bb_middle, bb_lower = self._compute_bollinger_bands(price, 20)
            indicators["bb_upper"] = bb_upper
            indicators["bb_middle"] = bb_middle
            indicators["bb_lower"] = bb_lower

            indicators["stochastic"] = self._compute_stochastic(price, high, low)

            return indicators

        except Exception as e:
            self.logger.error(f"Error computing advanced indicators: {str(e)}")
            return self._get_empty_indicators()

    def _get_empty_indicators(self) -> Dict[str, float]:
        return {
            "rsi_14": 50.0,
            "ema_12": 0.0,
            "ema_26": 0.0,
            "sma_50": 0.0,
            "sma_200": 0.0,
            "macd": 0.0,
            "macd_signal": 0.0,
            "macd_histogram": 0.0,
            "atr": 0.0,
            "vwap": 0.0,
            "bb_upper": 0.0,
            "bb_middle": 0.0,
            "bb_lower": 0.0,
            "stochastic": 50.0,
        }

    def _compute_rsi_advanced(self, high: float, low: float, close: float, period: int = 14) -> float:
        try:
            if close <= 0:
                return 50.0

            change = high - low
            gain = max(0, change * 0.02)
            loss = max(0, -change * 0.02)

            avg_gain = gain if gain > 0 else 0.01
            avg_loss = loss if loss > 0 else 0.01

            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

            return self._safe_float(max(0, min(100, rsi)))
        except Exception:
            return 50.0

    def _compute_ema_advanced(self, price: float, period: int) -> float:
        try:
            if price <= 0:
                return 0.0

            multiplier = 2.0 / (period + 1)
            ema = price * multiplier

            return self._safe_float(ema)
        except Exception:
            return price

    def _compute_atr_advanced(self, high: float, low: float, close: float, period: int = 14) -> float:
        try:
            if high <= 0 or low <= 0:
                return 0.0

            tr = max(high - low, abs(high - close), abs(low - close))
            atr = tr * 0.7

            return self._safe_float(max(0, atr))
        except Exception:
            return 0.0

    def _compute_vwap_advanced(self, price: float, volume: float) -> float:
        try:
            if volume <= 0:
                return price

            vwap = (price * volume) / max(volume, 1)
            return self._safe_float(vwap)
        except Exception:
            return price

    def _compute_bollinger_bands(
        self, price: float, period: int = 20, std_dev: float = 2.0
    ) -> Tuple[float, float, float]:
        try:
            if price <= 0:
                return price, price, price

            middle = price
            deviation = price * 0.05

            upper = middle + (deviation * std_dev)
            lower = middle - (deviation * std_dev)

            return self._safe_float(upper), self._safe_float(middle), self._safe_float(lower)
        except Exception:
            return price, price, price

    def _compute_stochastic(self, close: float, high: float, low: float, period: int = 14) -> float:
        try:
            if high <= low:
                return 50.0

            stoch = ((close - low) / (high - low)) * 100
            return self._safe_float(max(0, min(100, stoch)))
        except Exception:
            return 50.0

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
            market_summary["timestamp"] = datetime.now(timezone.utc).isoformat() + "Z"
            
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
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
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
            
            correlations["timestamp"] = datetime.now(timezone.utc).isoformat() + "Z"
            
            return correlations
        
        except Exception as e:
            self.logger.error(f"Error in compute_correlations: {str(e)}\n{traceback.format_exc()}")
            return {"status": "error", "timestamp": datetime.now(timezone.utc).isoformat() + "Z"}
    
    def enforce_schema(self, data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """Enforce unified output schema - single standardized format for all data"""
        try:
            if not data or not symbol:
                return {}
            
            schema = {
                "symbol": symbol.upper().strip(),
                "asset_type": data.get("type", "unknown"),
                "price": self._safe_float(data.get("price", 0)),
                "open": self._safe_float(data.get("open", 0)),
                "high": self._safe_float(data.get("high", 0)),
                "low": self._safe_float(data.get("low", 0)),
                "volume": self._safe_float(data.get("volume", 0)),
                "change": self._safe_float(data.get("change", 0)),
                "change_percent": self._safe_float(data.get("change_percent", 0)),
                "timestamp": data.get("timestamp", datetime.now(timezone.utc).isoformat() + "Z"),
            }
            
            features = data.get("basic_features", {})
            if features:
                schema["volatility"] = self._safe_float(features.get("volatility", 0))
            
            indicators = data.get("technical_indicators", {})
            if indicators:
                schema["sma_20"] = self._safe_float(indicators.get("sma_20", 0))
                schema["sma_50"] = self._safe_float(indicators.get("sma_50", 0))
                schema["rsi_14"] = self._safe_float(indicators.get("rsi_14", 50))
                schema["vwap"] = self._safe_float(indicators.get("vwap", schema["price"]))
            
            advanced = data.get("advanced_technical_indicators", {})
            if advanced:
                schema["ema_20"] = self._safe_float(advanced.get("ema_12", 0))
            
            return schema
        
        except Exception as e:
            self.logger.error(f"Error enforcing schema for {symbol}: {str(e)}")
            return {}
    def compute_sma(self, price: float, period: int = 20) -> float:
        """Compute Simple Moving Average (simplified - single price)"""
        try:
            if price <= 0:
                return 0.0
            return self._safe_float(price)
        except Exception:
            return 0.0
    
    def compute_ema(self, price: float, period: int = 20) -> float:
        """Compute Exponential Moving Average (simplified)"""
        try:
            if price <= 0:
                return 0.0
            multiplier = 2.0 / (period + 1)
            ema = price * multiplier
            return self._safe_float(ema)
        except Exception:
            return 0.0
    
    def compute_rsi(self, price: float, period: int = 14) -> float:
        """Compute Relative Strength Index (simplified)"""
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
        except Exception:
            return 50.0
    
    def compute_volatility_pct(self, high: float, low: float, price: float) -> float:
        """Compute volatility percentage"""
        try:
            if price <= 0 or high <= 0 or low <= 0:
                return 0.0
            
            price_range = high - low
            volatility = (price_range / price) * 100
            return self._safe_float(max(0, min(100, volatility)))
        except Exception:
            return 0.0
    
    def compute_vwap_basic(self, price: float, volume: float) -> float:
        """Compute Volume Weighted Average Price (simplified)"""
        try:
            if volume <= 0 or price <= 0:
                return price
            
            vwap = price
            return self._safe_float(vwap)
        except Exception:
            return price
    
    def add_basic_indicators(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add basic indicators to aggregated data"""
        try:
            price = self._safe_float(data.get("price", 0))
            high = self._safe_float(data.get("high", 0))
            low = self._safe_float(data.get("low", 0))
            volume = self._safe_float(data.get("volume", 0))
            
            indicators = {
                "sma_20": self.compute_sma(price, 20),
                "sma_50": self.compute_sma(price, 50),
                "ema_20": self.compute_ema(price, 20),
                "rsi_14": self.compute_rsi(price, 14),
                "volatility_pct": self.compute_volatility_pct(high, low, price),
                "vwap": self.compute_vwap_basic(price, volume),
            }
            
            data.update(indicators)
            return data
        
        except Exception as e:
            self.logger.error(f"Error adding basic indicators: {str(e)}")
            return data
    
    def compute_timeframe_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Compute multi-timeframe metrics (5min, 1hour aggregation)"""
        try:
            metrics = {
                "current": {
                    "price": self._safe_float(data.get("price", 0)),
                    "timestamp": data.get("timestamp", datetime.now(timezone.utc).isoformat() + "Z"),
                },
                "change_5min": 0.0,
                "change_1hour": 0.0,
                "volatility_5min": 0.0,
                "volatility_1hour": 0.0,
            }
            
            klines = data.get("klines", [])
            if klines and len(klines) >= 2:
                current_price = self._safe_float(data.get("price", 0))
                
                if len(klines) >= 1:
                    prev_price = self._safe_float(klines[-2][4]) if len(klines[-2]) > 4 else current_price
                    metrics["change_5min"] = ((current_price - prev_price) / prev_price * 100) if prev_price > 0 else 0
                
                if len(klines) >= 12:
                    prev_price_1h = self._safe_float(klines[-12][4]) if len(klines[-12]) > 4 else current_price
                    metrics["change_1hour"] = ((current_price - prev_price_1h) / prev_price_1h * 100) if prev_price_1h > 0 else 0
                
                prices_5 = [self._safe_float(k[4]) for k in klines[-5:] if len(k) > 4 and self._safe_float(k[4]) > 0]
                if len(prices_5) >= 2:
                    metrics["volatility_5min"] = self._safe_float(np.std(prices_5))
                
                prices_60 = [self._safe_float(k[4]) for k in klines[-60:] if len(k) > 4 and self._safe_float(k[4]) > 0]
                if len(prices_60) >= 2:
                    metrics["volatility_1hour"] = self._safe_float(np.std(prices_60))
            
            return metrics
        
        except Exception as e:
            self.logger.error(f"Error computing timeframe metrics: {str(e)}")
            return {}
    
    
    def apply_priority_merge(self, data_binance: Optional[Dict], data_alpha: Optional[Dict], symbol: str) -> Optional[Dict]:
        """Apply priority merging logic based on asset type"""
        try:
            asset_type = self._get_asset_type_from_symbol(symbol)
            if asset_type == "crypto":
                if data_binance and isinstance(data_binance, dict):
                    if self._safe_float(data_binance.get("price", 0)) > 0:
                        self.logger.debug(f"{symbol}: Using Binance (crypto priority)")
                        return data_binance
                
                if data_alpha and isinstance(data_alpha, dict):
                    if self._safe_float(data_alpha.get("price", 0)) > 0:
                        self.logger.debug(f"{symbol}: Fallback to Alpha Vantage (crypto)")
                        return data_alpha
            
            else:
                if data_alpha and isinstance(data_alpha, dict):
                    if self._safe_float(data_alpha.get("price", 0)) > 0:
                        self.logger.debug(f"{symbol}: Using Alpha Vantage (stock priority)")
                        return data_alpha
                
                if data_binance and isinstance(data_binance, dict):
                    if self._safe_float(data_binance.get("price", 0)) > 0:
                        self.logger.debug(f"{symbol}: Fallback to Binance (stock)")
                        return data_binance
            
            self.logger.warning(f"{symbol}: Both sources invalid, skipping")
            return None
        
        except Exception as e:
            self.logger.error(f"Error in priority merge for {symbol}: {str(e)}")
            return None
    
    def _get_asset_type_from_symbol(self, symbol: str) -> str:
        """Determine asset type from symbol"""
        if not symbol:
            return "unknown"
        
        s = symbol.upper().strip()
        crypto_indicators = ["BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "AVAX", 
                            "MATIC", "DOT", "LINK", "UNI", "LTC", "BCH", "NEAR"]
        
        if any(indicator in s for indicator in crypto_indicators):
            return "crypto"
        
        return "stock"
    
    def aggregate(self, raw_dict: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not isinstance(raw_dict, dict) or not raw_dict:
                self.logger.warning("aggregate received empty or invalid input")
                return self._get_empty_aggregate()
            
            normalized = self.normalize_all(raw_dict)
            
            aggregated_symbols = {}
            for symbol, norm_item in normalized.items():
                try:
                    enhanced_data = self.add_basic_indicators(norm_item)
                    
                    symbol_data = {
                        "normalized": norm_item,
                        "unified_schema": self.enforce_schema(enhanced_data, symbol),
                        "basic_features": self.compute_basic_features(norm_item),
                        "technical_indicators": self.compute_technical_indicators(norm_item),
                        "advanced_technical_indicators": self.compute_advanced_technical_indicators(norm_item),
                        "ml_features": self.compute_ml_features(norm_item)
                    }
                    
                    if norm_item.get("type") == "crypto" and norm_item.get("klines"):
                        symbol_data["kline_features"] = self.compute_kline_features(norm_item)
                        symbol_data["ohlcv_candles"] = self.compute_multi_window_ohlcv(norm_item, symbol)
                        symbol_data["timeframe_metrics"] = self.compute_timeframe_metrics(norm_item)
                    
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
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
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
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "metadata": {
                "total_symbols": 0,
                "crypto_count": 0,
                "stock_count": 0
            }
        }
    
    def prepare_excel_data(self, aggregated: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            if not aggregated or not aggregated.get("symbols"):
                self.logger.warning("prepare_excel_data: no data to process")
                return []
            
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
                        "timestamp": norm.get("timestamp", datetime.now(timezone.utc).isoformat())
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
                self.logger.warning("prepare_excel_data: no rows generated")
                return []
            
            return excel_rows
        
        except Exception as e:
            self.logger.error(f"Error in prepare_excel_data: {str(e)}\\n{traceback.format_exc()}")
            return []
    
    def prepare_ml_data(self, aggregated: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not aggregated or not aggregated.get("symbols"):
                self.logger.warning("prepare_ml_data: no data to process")
                return {}
            
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
                            "timestamp": norm.get("timestamp", datetime.now(timezone.utc).isoformat())
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
                self.logger.warning("prepare_ml_data: no data prepared")
                return {}
            
            market_summary = aggregated.get("market_summary", {})
            correlations = aggregated.get("correlations", {})
            
            full_ml_data = {
                "symbols": ml_ready_data,
                "market_summary": market_summary,
                "correlations": correlations,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            return full_ml_data
        
        except Exception as e:
            self.logger.error(f"Error in prepare_ml_data: {str(e)}\\n{traceback.format_exc()}")
            return {}
    
    @correlation_decorator()
    def run(self, raw_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            self.logger.info(f"Starting aggregation for {len(raw_dict)} symbols")
            
            aggregated = self.aggregate(raw_dict)
            
            if aggregated.get("metadata", {}).get("total_symbols", 0) > 0:
                excel_rows = self.prepare_excel_data(aggregated)
                ml_data = self.prepare_ml_data(aggregated)
                
                if excel_rows:
                    aggregated["excel_rows"] = excel_rows
                
                if ml_data:
                    aggregated["ml_data"] = ml_data
                
                log_metric("run_complete", 1, {
                    "symbols": len(aggregated.get("symbols", {})),
                    "excel_rows_count": len(excel_rows),
                    "ml_data_present": bool(ml_data)
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
                    "timestamp": datetime.now(timezone.utc).isoformat()
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

    def generate_ml_ready_row(self, symbol: str, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Generate unified ML-ready row for single symbol - single standardized format"""
        try:
            if not item or not symbol:
                return None
            
            price = self._safe_float(item.get("price", 0))
            if price <= 0:
                return None
            
            basic_features = self.compute_basic_features(item)
            tech_indicators = self.compute_technical_indicators(item)
            advanced_indicators = self.compute_advanced_technical_indicators(item)
            kline_features = self.compute_kline_features(item)
            ml_features = self.compute_ml_features(item)
            
            ml_row = {
                "symbol": symbol,
                "asset_type": item.get("type", "unknown"),
                "timestamp": item.get("timestamp", datetime.now(timezone.utc).isoformat() + "Z"),
                
                "price": price,
                "open": self._safe_float(item.get("open", 0)),
                "high": self._safe_float(item.get("high", 0)),
                "low": self._safe_float(item.get("low", 0)),
                "volume": self._safe_float(item.get("volume", 0)),
                
                "price_diff": basic_features.get("price_diff", 0),
                "pct_change": basic_features.get("pct_change", 0),
                "price_range": basic_features.get("price_range", 0),
                "price_position": basic_features.get("price_position", 50),
                
                "volatility": basic_features.get("volatility", 0),
                
                "rsi_14": advanced_indicators.get("rsi_14", 50),
                "ema_12": advanced_indicators.get("ema_12", 0),
                "ema_26": advanced_indicators.get("ema_26", 0),
                "sma_50": advanced_indicators.get("sma_50", 0),
                "sma_200": advanced_indicators.get("sma_200", 0),
                "macd": advanced_indicators.get("macd", 0),
                "macd_signal": advanced_indicators.get("macd_signal", 0),
                "macd_histogram": advanced_indicators.get("macd_histogram", 0),
                "atr": advanced_indicators.get("atr", 0),
                "vwap": advanced_indicators.get("vwap", price),
                "bb_upper": advanced_indicators.get("bb_upper", 0),
                "bb_middle": advanced_indicators.get("bb_middle", 0),
                "bb_lower": advanced_indicators.get("bb_lower", 0),
                "stochastic": advanced_indicators.get("stochastic", 50),
                
                "rolling_mean_24h": kline_features.get("rolling_mean_24h", 0),
                "rolling_std_24h": kline_features.get("rolling_std_24h", 0),
                "rolling_volume_sum_24h": kline_features.get("rolling_volume_sum_24h", 0),
                "rolling_volume_change": kline_features.get("rolling_volume_change", 0),
                
                "momentum": ml_features.get("momentum", 0),
                "trend_strength": ml_features.get("trend_strength", 0),
                "volatility_normalized": ml_features.get("volatility_normalized", 0),
                "volume_normalized": ml_features.get("volume_normalized", 0),
                "data_quality_score": ml_features.get("data_quality_score", 0),
            }
            
            return ml_row
        
        except Exception as e:
            self.logger.error(f"Error generating ML row for {symbol}: {str(e)}")
            return None
    
    def generate_ml_ready_batch(self, aggregated: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate ML-ready rows for all symbols in batch"""
        try:
            ml_rows = []
            
            symbols_data = aggregated.get("symbols", {})
            for symbol, data in symbols_data.items():
                try:
                    normalized = data.get("normalized", {})
                    ml_row = self.generate_ml_ready_row(symbol, normalized)
                    if ml_row:
                        ml_rows.append(ml_row)
                except Exception as e:
                    self.logger.warning(f"Skipping ML row for {symbol}: {str(e)}")
                    continue
            
            self.logger.info(f"Generated {len(ml_rows)} ML-ready rows")
            return ml_rows
        
        except Exception as e:
            self.logger.error(f"Error generating ML batch: {str(e)}")
            return []
    
    def compute_volatility_window(self, item: Dict[str, Any], window_minutes: int = 1440) -> Dict[str, Any]:
        """Compute volatility metrics over time window from klines"""
        try:
            klines = item.get("klines", [])
            if not klines:
                return {"window_minutes": window_minutes, "volatility_pct": 0, "price_change_window": 0, "status": "no_klines"}
            
            try:
                prices = []
                for kline in klines:
                    if isinstance(kline, (list, tuple)) and len(kline) >= 5:
                        price = self._safe_float(kline[4])
                        if price and price > 0:
                            prices.append(price)
                
                if len(prices) < 2:
                    return {"window_minutes": window_minutes, "volatility_pct": 0, "price_change_window": 0, "status": "insufficient_data"}
                
                volatility_pct = (np.std(prices) / np.mean(prices) * 100) if np.mean(prices) > 0 else 0
                price_change_window = ((prices[-1] - prices[0]) / prices[0] * 100) if prices[0] > 0 else 0
                
                return {
                    "window_minutes": window_minutes,
                    "volatility_pct": self._safe_float(volatility_pct),
                    "price_change_window": self._safe_float(price_change_window),
                    "kline_count": len(prices),
                    "status": "ok"
                }
            
            except Exception as e:
                self.logger.debug(f"Error in volatility window computation: {str(e)}")
                return {"window_minutes": window_minutes, "volatility_pct": 0, "price_change_window": 0, "status": "error"}
        
        except Exception as e:
            self.logger.error(f"Error computing volatility window: {str(e)}")
            return {}
    
    def compute_daily_change(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Compute daily aggregated metrics from klines"""
        try:
            klines = item.get("klines", [])
            if not klines:
                return {"daily_open": None, "daily_close": None, "daily_change": 0, "daily_change_pct": 0, "status": "no_klines"}
            
            try:
                prices = []
                for kline in klines:
                    if isinstance(kline, (list, tuple)) and len(kline) >= 5:
                        price = self._safe_float(kline[4])
                        if price and price > 0:
                            prices.append(price)
                
                if not prices:
                    return {"daily_open": None, "daily_close": None, "daily_change": 0, "daily_change_pct": 0, "status": "no_valid_prices"}
                
                daily_open = prices[0]
                daily_close = prices[-1]
                daily_change = daily_close - daily_open
                daily_change_pct = (daily_change / daily_open * 100) if daily_open > 0 else 0
                
                return {
                    "daily_open": self._safe_float(daily_open),
                    "daily_close": self._safe_float(daily_close),
                    "daily_change": self._safe_float(daily_change),
                    "daily_change_pct": self._safe_float(daily_change_pct),
                    "kline_count": len(prices),
                    "status": "ok"
                }
            
            except Exception as e:
                self.logger.debug(f"Error computing daily change: {str(e)}")
                return {}
        
        except Exception as e:
            self.logger.error(f"Error in compute_daily_change: {str(e)}")
            return {}
    
    def merge_with_fallback(self, primary: Optional[Dict[str, Any]], backup: Optional[Dict[str, Any]], symbol: str) -> Optional[Dict[str, Any]]:
        """Merge data with fallback logic - primary preferred, fallback to backup if primary invalid"""
        try:
            if primary and isinstance(primary, dict):
                if primary.get("price", 0) > 0:
                    self.logger.debug(f"{symbol}: Using primary data")
                    return primary
            
            if backup and isinstance(backup, dict):
                if backup.get("price", 0) > 0:
                    self.logger.debug(f"{symbol}: Falling back to backup data")
                    return backup
            
            # Both invalid
            self.logger.warning(f"{symbol}: Both primary and backup data invalid, skipping")
            return None
        
        except Exception as e:
            self.logger.error(f"Error in merge_with_fallback for {symbol}: {str(e)}")
            return None
