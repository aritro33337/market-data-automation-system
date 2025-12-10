import pytest
from unittest.mock import patch, MagicMock
from src.core.aggregator import DataAggregator, TimeWindow, OHLCVCandle
from datetime import datetime
import numpy as np


class TestDataAggregator:

    @pytest.fixture
    def aggregator(self):
        with patch('src.core.aggregator.ConfigLoader.load') as mock_config:
            mock_config.return_value = {
                "data_sources": {
                    "processed_data_path": "data/processed",
                },
                "export": {
                    "excel_output_folder": "data/exports",
                    "excel_file_prefix": "market_data_",
                    "include_timestamp_in_filename": True
                }
            }
            yield DataAggregator()

    def test_initialization(self, aggregator):
        assert aggregator is not None
        assert aggregator.config is not None
        assert aggregator.normalized_cache == {}
        assert aggregator.feature_cache == {}

    def test_safe_float(self, aggregator):
        assert aggregator._safe_float(10.5) == 10.5
        assert aggregator._safe_float("20.3") == 20.3
        assert aggregator._safe_float(None) == 0.0
        assert aggregator._safe_float(None, 5.0) == 5.0
        assert aggregator._safe_float("invalid") == 0.0

    def test_safe_int(self, aggregator):
        assert aggregator._safe_int(10) == 10
        assert aggregator._safe_int("20") == 20
        assert aggregator._safe_int(None) == 0
        assert aggregator._safe_int(None, 5) == 5
        assert aggregator._safe_int("invalid") == 0

    def test_is_crypto_detection(self, aggregator):
        assert aggregator._is_crypto("BTC") is True
        assert aggregator._is_crypto("ETH") is True
        assert aggregator._is_crypto("BTCUSDT") is True
        assert aggregator._is_crypto("AAPL") is False
        assert aggregator._is_crypto("MSFT") is False
        assert aggregator._is_crypto("") is False
        assert aggregator._is_crypto(None) is False

    def test_normalize_single_stock(self, aggregator):
        raw_item = {
            "price": "150.0",
            "open": "149.5",
            "high": "151.0",
            "low": "149.0",
            "volume": "1000000",
            "timestamp": "2025-12-10T10:00:00Z",
            "api": "alpha_vantage"
        }

        result = aggregator.normalize_single(raw_item, "AAPL")
        assert result is not None
        assert result["symbol"] == "AAPL"
        assert result["type"] == "stock"
        assert result["price"] == 150.0
        assert result["api"] == "alpha_vantage"

    def test_normalize_single_crypto(self, aggregator):
        raw_item = {
            "price": "45000.50",
            "open": "44500.0",
            "high": "46000.0",
            "low": "44000.0",
            "volume": "1000.5",
            "timestamp": "2025-12-10T10:00:00Z",
            "api": "binance"
        }

        result = aggregator.normalize_single(raw_item, "BTC")
        assert result is not None
        assert result["symbol"] == "BTC"
        assert result["type"] == "crypto"
        assert result["price"] == 45000.50

    def test_normalize_single_invalid_input(self, aggregator):
        assert aggregator.normalize_single(None, "TEST") is None
        assert aggregator.normalize_single({}, None) is None
        assert aggregator.normalize_single("invalid", "TEST") is None

    def test_compute_basic_features(self, aggregator):
        item = {
            "price": 150.0,
            "open": 149.5,
            "high": 151.0,
            "low": 149.0,
            "volume": 1000000.0
        }

        features = aggregator.compute_basic_features(item)
        assert features is not None
        assert "price_diff" in features
        assert "pct_change" in features
        assert "price_range" in features
        assert "volatility" in features
        assert features["price_range"] == 2.0

    def test_compute_technical_indicators(self, aggregator):
        item = {
            "price": 150.0,
            "open": 149.5,
            "high": 151.0,
            "low": 149.0,
            "volume": 1000000.0
        }

        indicators = aggregator.compute_technical_indicators(item)
        assert indicators is not None
        assert "rsi_14" in indicators
        assert "ema_12" in indicators
        assert "ema_26" in indicators
        assert "macd" in indicators
        assert "atr" in indicators
        assert "vwap" in indicators

    def test_compute_advanced_technical_indicators(self, aggregator):
        item = {
            "price": 150.0,
            "open": 149.5,
            "high": 151.0,
            "low": 149.0,
            "volume": 1000000.0
        }

        indicators = aggregator.compute_advanced_technical_indicators(item)
        assert indicators is not None
        assert "rsi_14" in indicators
        assert "ema_12" in indicators
        assert "macd" in indicators
        assert "bb_upper" in indicators
        assert "bb_lower" in indicators
        assert "stochastic" in indicators

    def test_compute_ml_features(self, aggregator):
        item = {
            "price": 150.0,
            "open": 149.5,
            "high": 151.0,
            "low": 149.0,
            "volume": 1000000.0,
            "change_percent": 0.5
        }

        features = aggregator.compute_ml_features(item)
        assert features is not None
        assert "momentum" in features
        assert "trend_strength" in features
        assert "volatility_normalized" in features
        assert "volume_normalized" in features
        assert "price_position_ratio" in features
        assert "data_quality_score" in features

    def test_ohlcv_candle_creation(self, aggregator):
        candle = OHLCVCandle("1h")
        candle.open = 100.0
        candle.high = 105.0
        candle.low = 99.0
        candle.close = 104.0
        candle.volume = 1000.0
        candle.timestamp = datetime.utcnow().isoformat()

        candle_dict = candle.to_dict()
        assert candle_dict["window"] == "1h"
        assert candle_dict["open"] == 100.0
        assert candle_dict["high"] == 105.0
        assert candle_dict["low"] == 99.0
        assert candle_dict["close"] == 104.0
        assert candle_dict["volume"] == 1000.0

    def test_compute_ohlcv_from_klines(self, aggregator):
        klines = [
            [1000, "100.0", "105.0", "99.0", "104.0", "1000.0", 0, "1000000.0"],
            [2000, "101.0", "106.0", "100.0", "105.0", "1500.0", 0, "1500000.0"],
            [3000, "102.0", "107.0", "101.0", "106.0", "2000.0", 0, "2000000.0"],
        ]

        result = aggregator.compute_ohlcv_from_klines(klines, "BTC", "1h")
        assert result is not None
        assert result["open"] == 104.0
        assert result["close"] == 106.0
        assert result["high"] == 106.0
        assert result["low"] == 104.0
        assert result["volume"] == 4500000.0

    def test_compute_ohlcv_empty_klines(self, aggregator):
        result = aggregator.compute_ohlcv_from_klines([], "BTC", "1h")
        assert result is None

        result = aggregator.compute_ohlcv_from_klines(None, "BTC", "1h")
        assert result is None

    def test_compute_market_wide_features(self, aggregator):
        all_items = {
            "BTC": {
                "type": "crypto",
                "price": 45000.0,
                "high": 46000.0,
                "low": 44000.0,
                "volume": 1000.0,
                "change_percent": 1.5
            },
            "AAPL": {
                "type": "stock",
                "price": 150.0,
                "high": 151.0,
                "low": 149.0,
                "volume": 1000000.0,
                "change_percent": 0.5
            }
        }

        summary = aggregator.compute_market_wide_features(all_items)
        assert summary is not None
        assert "crypto_strength" in summary
        assert "stock_strength" in summary
        assert "sector_divergence" in summary
        assert "global_volatility" in summary
        assert "total_volume" in summary
        assert "asset_count" in summary
        assert summary["asset_count"] == 2

    def test_compute_correlations(self, aggregator):
        all_items = {
            "BTC": {
                "type": "crypto",
                "change_percent": 2.0,
                "volatility": 1.5
            },
            "ETH": {
                "type": "crypto",
                "change_percent": 1.8,
                "volatility": 1.4
            },
            "AAPL": {
                "type": "stock",
                "change_percent": 0.5,
                "volatility": 0.8
            }
        }

        correlations = aggregator.compute_correlations(all_items)
        assert correlations is not None
        assert "timestamp" in correlations

    def test_aggregate_multi_symbols(self, aggregator):
        raw_dict = {
            "BTC": {
                "price": 45000.0,
                "open": 44500.0,
                "high": 46000.0,
                "low": 44000.0,
                "volume": 1000.0,
                "type": "crypto",
                "api": "binance",
                "timestamp": "2025-12-10T10:00:00Z"
            },
            "AAPL": {
                "price": 150.0,
                "open": 149.5,
                "high": 151.0,
                "low": 149.0,
                "volume": 1000000.0,
                "type": "stock",
                "api": "alpha_vantage",
                "timestamp": "2025-12-10T10:00:00Z"
            }
        }

        result = aggregator.aggregate(raw_dict)
        assert result is not None
        assert "symbols" in result
        assert "market_summary" in result
        assert "correlations" in result
        assert "metadata" in result
        assert result["metadata"]["total_symbols"] == 2
        assert result["metadata"]["crypto_count"] == 1
        assert result["metadata"]["stock_count"] == 1

    def test_aggregate_empty_input(self, aggregator):
        result = aggregator.aggregate({})
        assert result is not None
        assert result["metadata"]["total_symbols"] == 0

        result = aggregator.aggregate(None)
        assert result is not None
        assert result["metadata"]["total_symbols"] == 0

    def test_cache_operations(self, aggregator):
        raw_dict = {
            "TEST": {
                "price": 100.0,
                "open": 99.0,
                "high": 101.0,
                "low": 98.0,
                "volume": 1000.0,
                "type": "stock",
                "api": "test",
                "timestamp": "2025-12-10T10:00:00Z"
            }
        }

        aggregator.aggregate(raw_dict)
        
        cache_status = aggregator.get_cache_status()
        assert cache_status is not None
        assert cache_status["normalized_cache_size"] >= 0

        aggregator.clear_cache()
        assert len(aggregator.normalized_cache) == 0
        assert len(aggregator.feature_cache) == 0

    def test_compute_rsi_advanced(self, aggregator):
        rsi = aggregator._compute_rsi_advanced(105.0, 99.0, 104.0, 14)
        assert isinstance(rsi, float)
        assert 0 <= rsi <= 100

    def test_compute_ema_advanced(self, aggregator):
        ema = aggregator._compute_ema_advanced(100.0, 12)
        assert isinstance(ema, float)
        assert ema >= 0

    def test_compute_bollinger_bands(self, aggregator):
        upper, middle, lower = aggregator._compute_bollinger_bands(100.0, 20, 2.0)
        assert upper >= middle
        assert middle >= lower
        assert middle == 100.0

    def test_compute_stochastic(self, aggregator):
        stoch = aggregator._compute_stochastic(104.0, 105.0, 99.0, 14)
        assert isinstance(stoch, float)
        assert 0 <= stoch <= 100

    def test_normalize_all_multiple_symbols(self, aggregator):
        raw_dict = {
            "BTC": {
                "price": "45000.0",
                "open": "44500.0",
                "high": "46000.0",
                "low": "44000.0",
                "volume": "1000.0",
                "timestamp": "2025-12-10T10:00:00Z",
                "api": "binance"
            },
            "AAPL": {
                "price": "150.0",
                "open": "149.5",
                "high": "151.0",
                "low": "149.0",
                "volume": "1000000.0",
                "timestamp": "2025-12-10T10:00:00Z",
                "api": "alpha_vantage"
            }
        }

        result = aggregator.normalize_all(raw_dict)
        assert len(result) == 2
        assert "BTC" in result
        assert "AAPL" in result
        assert result["BTC"]["type"] == "crypto"
        assert result["AAPL"]["type"] == "stock"

    def test_prepare_excel_data(self, aggregator):
        aggregated = {
            "symbols": {
                "AAPL": {
                    "normalized": {"price": 150.0},
                    "basic_features": {},
                    "technical_indicators": {},
                    "ml_features": {},
                    "kline_features": {}
                }
            }
        }
        
        result = aggregator.prepare_excel_data(aggregated)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"

    def test_prepare_ml_data(self, aggregator):
        aggregated = {
            "symbols": {
                "AAPL": {
                    "normalized": {"price": 150.0},
                    "ml_features": {"momentum": 0.5}
                }
            }
        }
        
        result = aggregator.prepare_ml_data(aggregated)
        assert isinstance(result, dict)
        assert "symbols" in result
        assert "AAPL" in result["symbols"]
