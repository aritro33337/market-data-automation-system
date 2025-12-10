import pytest
import time
from unittest.mock import Mock, MagicMock, patch
from src.core.live_data_fetcher import LiveDataFetcher, AssetType
import json


class TestLiveDataFetcher:

    @pytest.fixture
    def fetcher(self):
        with patch('src.core.live_data_fetcher.ConfigLoader.load') as mock_config:
            mock_config.return_value = {
                "data_sources": {
                    "symbols": ["AAPL", "BTC"],
                    "fallback_symbols": ["MSFT", "ETH"],
                    "max_retry": 3,
                    "retry_delay_seconds": 0.5,
                    "raw_data_path": "data/raw",
                    "processed_data_path": "data/processed"
                },
                "api": {
                    "api_key": "test_key",
                    "timeout_seconds": 10,
                    "rate_limit_delay": 0.1
                },
                "crypto_features": {
                    "fetch_klines": True,
                    "fetch_trades": False,
                    "fetch_orderbook": False
                }
            }
            yield LiveDataFetcher()

    def test_initialization(self, fetcher):
        assert fetcher is not None
        assert fetcher.config is not None
        assert fetcher.session is not None
        assert len(fetcher.symbols) > 0
        assert fetcher.max_retries == 3

    def test_asset_type_detection_stock(self, fetcher):
        assert fetcher._get_asset_type("AAPL") == AssetType.STOCK
        assert fetcher._get_asset_type("MSFT") == AssetType.STOCK
        assert fetcher._get_asset_type("GOOGL") == AssetType.STOCK

    def test_asset_type_detection_crypto(self, fetcher):
        assert fetcher._get_asset_type("BTC") == AssetType.CRYPTO
        assert fetcher._get_asset_type("ETH") == AssetType.CRYPTO
        assert fetcher._get_asset_type("BTCUSDT") == AssetType.CRYPTO

    def test_safe_float(self, fetcher):
        assert fetcher._safe_float(10.5) == 10.5
        assert fetcher._safe_float("20.3") == 20.3
        assert fetcher._safe_float(None) is None
        assert fetcher._safe_float(None, 0.0) == 0.0
        assert fetcher._safe_float("invalid") is None

    def test_safe_int(self, fetcher):
        assert fetcher._safe_int(10) == 10
        assert fetcher._safe_int("20") == 20
        assert fetcher._safe_int(None) == 0
        assert fetcher._safe_int(None, 5) == 5
        assert fetcher._safe_int("invalid") == 0

    def test_exponential_backoff_calculation(self, fetcher):
        backoff_1 = fetcher._calculate_backoff(0, 0.5)
        backoff_2 = fetcher._calculate_backoff(1, 0.5)
        backoff_3 = fetcher._calculate_backoff(2, 0.5)

        assert backoff_1 >= 0.5
        assert backoff_2 >= 1.0
        assert backoff_3 >= 2.0

    def test_crypto_symbol_normalization(self, fetcher):
        assert fetcher._normalize_crypto_symbol("BTCUSDT") == "BTC"
        assert fetcher._normalize_crypto_symbol("ETHBUSD") == "ETH"
        assert fetcher._normalize_crypto_symbol("BTC") == "BTC"
        assert fetcher._normalize_crypto_symbol("") == ""

    def test_validate_api_response_valid(self, fetcher):
        response_data = {
            "symbol": "BTCUSDT",
            "lastPrice": "45000.50",
            "highPrice": "46000.00",
            "lowPrice": "44000.00"
        }

        result = fetcher._validate_api_response(response_data, "BTCUSDT", "binance")
        assert result is not None
        assert "lastPrice" in response_data or "symbol" in result

    def test_validate_api_response_invalid_price(self, fetcher):
        response_data = {
            "symbol": "BTCUSDT",
            "lastPrice": "-5",
            "highPrice": "46000.00"
        }

        result = fetcher._validate_api_response(response_data, "BTC", "binance")
        assert result is None

    def test_validate_binance_response_valid(self, fetcher):
        response_data = {
            "symbol": "BTCUSDT",
            "lastPrice": "45000.50",
            "highPrice": "46000.00",
            "lowPrice": "44000.00",
            "openPrice": "44500.00",
            "volume": "1000.5",
            "quoteAssetVolume": "45000000",
            "priceChange": "500.50",
            "priceChangePercent": "1.12",
            "bidPrice": "45000.00",
            "askPrice": "45001.00",
            "count": 50000,
            "weightedAvgPrice": "45000.75"
        }

        result = fetcher._validate_binance_response(response_data, "BTC")
        assert result is not None
        assert result["price"] == 45000.50
        assert result["asset_type"] == "crypto"

    def test_validate_binance_response_invalid_price(self, fetcher):
        response_data = {
            "symbol": "BTCUSDT",
            "lastPrice": "-100"
        }

        result = fetcher._validate_binance_response(response_data, "BTC")
        assert result is None

    def test_normalize_api_response_binance(self, fetcher):
        data = {
            "lastPrice": "100.5",
            "volume": "1000",
            "highPrice": "101.0",
            "lowPrice": "100.0"
        }

        result = fetcher.normalize_api_response(data, "BTC", "binance")
        assert result is not None
        assert result["symbol"] == "BTC"
        assert result["source_api"] == "binance"
        assert result["price"] == 100.5

    def test_normalize_api_response_alpha_vantage(self, fetcher):
        data = {
            "Global Quote": {
                "05. price": "150.0",
                "06. volume": "1000000",
                "03. high": "151.0",
                "04. low": "149.0",
                "02. open": "150.0"
            }
        }

        result = fetcher.normalize_api_response(data, "AAPL", "alpha_vantage")
        assert result is not None
        assert result["symbol"] == "AAPL"
        assert result["source_api"] == "alpha_vantage"
        assert result["price"] == 150.0

    def test_health_check(self, fetcher):
        health = fetcher.health_check()
        assert health is not None
        assert health["status"] == "healthy"
        assert health["session_active"] is True
        assert health["config_loaded"] is True
        assert health["symbols_count"] > 0

    def test_cache_operations(self, fetcher):
        fetcher.cache["TEST"] = ("data", time.time())
        
        stats = fetcher.get_cache_stats()
        assert stats["cache_size"] > 0
        assert "TEST" in stats["cache_items"]

        fetcher.clear_cache()
        assert len(fetcher.cache) == 0

    def test_rate_limit_enforcement(self, fetcher):
        initial_requests = len(fetcher.alpha_requests)
        fetcher._enforce_rate_limit("alpha_vantage")
        assert len(fetcher.alpha_requests) > initial_requests

    def test_build_url_binance(self, fetcher):
        url, api_type = fetcher._build_url("BTC", "ticker")
        assert api_type == "binance"
        assert "BTCUSDT" in url
        assert "ticker/24hr" in url

    def test_build_url_alpha_vantage(self, fetcher):
        url, api_type = fetcher._build_url("AAPL", "ticker")
        assert api_type == "alpha_vantage"
        assert "AAPL" in url
        assert "GLOBAL_QUOTE" in url

    def test_build_url_klines(self, fetcher):
        url, api_type = fetcher._build_url("BTC", "klines")
        assert api_type == "binance"
        assert "klines" in url

    def test_error_handling_timeout(self, fetcher):
        with patch('src.core.live_data_fetcher.requests.Session.get') as mock_get:
            mock_get.side_effect = TimeoutError("Timeout")
            result = fetcher._make_request("http://test.com", "TEST", "alpha_vantage")
            assert result is None

    def test_error_handling_json_decode(self, fetcher):
        with patch('src.core.live_data_fetcher.requests.Session.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.side_effect = json.JSONDecodeError("msg", "doc", 0)
            mock_get.return_value = mock_response
            result = fetcher._make_request("http://test.com", "TEST", "alpha_vantage")
            assert result is None

    def test_invalid_symbol_fetch(self, fetcher):
        result = fetcher.fetch_single_symbol(None)
        assert result is None

        result = fetcher.fetch_single_symbol("")
        assert result is None

    def test_extract_percent(self, fetcher):
        assert fetcher._extract_percent("1.5%") == 1.5
        assert fetcher._extract_percent("-2.3%") == -2.3
        assert fetcher._extract_percent(None) is None
        assert fetcher._extract_percent("invalid") is None
