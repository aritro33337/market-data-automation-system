from src.utils.logger import get_logger, log_metric, correlation_decorator
from src.utils.config_loader import ConfigLoader
import requests
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone
import os
import json
from enum import Enum
import re
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry
import threading
import random
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf


class AssetType(Enum):
    STOCK = "stock"
    CRYPTO = "crypto"


class LiveDataFetcher:
    STOCK_SYMBOLS = [
        "AAPL",
        "MSFT",
        "GOOGL",
        "AMZN",
        "NVDA",
        "TSLA",
        "META",
        "NFLX",
        "ORCL",
        "INTC",
        "AMD",
        "QCOM",
        "CSCO",
        "IBM",
        "JPM",
        "BAC",
        "WMT",
        "JNJ",
        "PG",
        "XOM",
    ]
    CRYPTO_SYMBOLS = [
        "BTC",
        "ETH",
        "BNB",
        "SOL",
        "XRP",
        "ADA",
        "DOGE",
        "AVAX",
        "MATIC",
        "DOT",
        "LINK",
        "UNI",
        "LTC",
        "BCH",
        "NEAR",
    ]

    CRYPTO_SYMBOLS = [
        "BTC",
        "ETH",
        "BNB",
        "SOL",
        "XRP",
        "ADA",
        "DOGE",
        "AVAX",
        "MATIC",
        "DOT",
        "LINK",
        "UNI",
        "LTC",
        "BCH",
        "NEAR",
    ]

    BINANCE_URL = "https://api.binance.com/api/v3"

    def __init__(self, config_path: str = "config/settings.json"):
        self.logger = get_logger("LiveDataFetcher")
        self.config = None
        self.session = None
        self.symbols = []
        self.fallback_symbols = []
        self.api_key = None
        self.max_retries = 3
        self.timeout = 30
        self.retry_delay = 2
        self.rate_limit_delay = 0.5
        self.cache = {}
        self.cache_ttl = 300
        self.cache_timestamps = {}
        self.cache_lock = threading.RLock()

        self.cache_lock = threading.RLock()

        self.binance_requests = []
        self.max_binance_requests_per_minute = 1200
        self.rate_limit_lock = threading.RLock()

        self.fetch_klines = True
        self.fetch_trades = False
        self.fetch_orderbook = False

        try:
            self._load_and_validate_config(config_path)
            self._initialize_session()
            self.logger.info(
                "LiveDataFetcher initialized (Dual API: Alpha Vantage + Binance, Enhanced)"
            )
            log_metric(
                "live_fetcher_init",
                1,
                {
                    "status": "success",
                    "apis": ["yfinance", "binance"],
                    "klines": self.fetch_klines,
                    "trades": self.fetch_trades,
                    "orderbook": self.fetch_orderbook,
                },
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize LiveDataFetcher: {str(e)}")
            log_metric("live_fetcher_init", 0, {"status": "failed", "error": str(e)})
            raise

    def _load_and_validate_config(self, config_path: str) -> None:
        try:
            self.config = ConfigLoader.load(config_path)

            if not self.config:
                raise ValueError("Configuration is empty")

            data_sources = self.config.get("data_sources", {})
            if not data_sources:
                raise ValueError("data_sources configuration missing")

            self.symbols = data_sources.get("symbols", [])
            self.fallback_symbols = data_sources.get("fallback_symbols", [])

            if not self.symbols:
                self.logger.warning("No symbols configured, using defaults")
                self.symbols = ["AAPL", "TSLA", "BTC", "ETH"]

            self.api_key = None # Alpha Vantage key no longer needed

            self.max_retries = data_sources.get("max_retry", 3)
            self.timeout = self.config.get("api", {}).get("timeout_seconds", 30)
            self.retry_delay = data_sources.get("retry_delay_seconds", 2)
            self.rate_limit_delay = self.config.get("api", {}).get(
                "rate_limit_delay", 0.5
            )

            crypto_features = self.config.get("crypto_features", {})
            self.fetch_klines = crypto_features.get("fetch_klines", True)
            self.fetch_trades = crypto_features.get("fetch_trades", False)
            self.fetch_orderbook = crypto_features.get("fetch_orderbook", False)

            self.logger.info(
                f"Config: {len(self.symbols)} symbols, Dual API, Klines={self.fetch_klines}, Trades={self.fetch_trades}, Orderbook={self.fetch_orderbook}"
            )
            log_metric(
                "config_load",
                1,
                {
                    "symbols_count": len(self.symbols),
                    "apis": "dual",
                    "klines": self.fetch_klines,
                    "trades": self.fetch_trades,
                },
            )

        except Exception as e:
            self.logger.error(f"Config validation failed: {str(e)}")
            log_metric("config_load", 0, {"error": str(e)})
            raise

    def _normalize_crypto_symbol(self, symbol: str) -> str:
        if not symbol or not isinstance(symbol, str):
            return symbol

        s = symbol.upper().strip()
        parts = re.findall(r"[A-Z0-9]+", s)
        if not parts:
            return symbol

        last_part = parts[-1]

        if last_part in ("USD", "USDT", "BUSD"):
            s = parts[0] if parts else symbol
        else:
            s = last_part
            for suffix in ("USDT", "BUSD", "USD"):
                if s.endswith(suffix):
                    s = s[: -len(suffix)]
                    break

        s = s.strip()
        if s in self.CRYPTO_SYMBOLS:
            return s
        if symbol.upper().strip() in self.CRYPTO_SYMBOLS:
            return symbol.upper().strip()

        return s

    def _get_asset_type(self, symbol: str) -> AssetType:
        if not symbol or not isinstance(symbol, str):
            return AssetType.STOCK

        s = symbol.upper().strip()
        if s in self.CRYPTO_SYMBOLS:
            return AssetType.CRYPTO

        if s.endswith(("USDT", "USD", "BUSD")):
            return AssetType.CRYPTO

        s_clean = re.sub(r"[^A-Z0-9]", "", s)
        for suffix in ("USDT", "BUSD", "USD"):
            if s_clean.endswith(suffix):
                base = s_clean[: -len(suffix)]
                if base in self.CRYPTO_SYMBOLS:
                    return AssetType.CRYPTO

        return AssetType.STOCK

    def _initialize_session(self) -> None:
        try:
            self.session = requests.Session()

            retry_strategy = Retry(
                total=5,
                backoff_factor=0.8,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods={"GET", "POST"},
            )

            adapter = requests.adapters.HTTPAdapter(
                pool_connections=20, pool_maxsize=20, max_retries=retry_strategy
            )

            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)

            self.session.headers.update(
                {
                    "User-Agent": "MarketDataAutomationSystem/1.0",
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                }
            )

            self.logger.info("HTTP session initialized (20x20 pooling, retry strategy)")
        except Exception as e:
            self.logger.error(f"Failed to initialize HTTP session: {str(e)}")
            raise

    def _safe_float(
        self, value: Any, default: Optional[float] = None
    ) -> Optional[float]:
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

    def _enforce_rate_limit(self, api_type: str) -> None:
        with self.rate_limit_lock:
            current_time = time.time()
            minute_ago = current_time - 60

            if api_type == "binance":
                self.binance_requests = [
                    t for t in self.binance_requests if t > minute_ago
                ]

                if len(self.binance_requests) >= self.max_binance_requests_per_minute:
                    sleep_time = 60 - (current_time - self.binance_requests[0]) + 0.5
                    self.logger.warning(
                        f"Binance rate limit: sleeping {sleep_time:.1f}s"
                    )
                    time.sleep(sleep_time)
                    self.binance_requests = []

                self.binance_requests.append(current_time)

    def _build_url(self, symbol: str, endpoint: str = "ticker") -> Tuple[str, str]:
        try:
            symbol = symbol.upper().strip()
            asset_type = self._get_asset_type(symbol)

            if asset_type == AssetType.CRYPTO:
                symbol_normalized = self._normalize_crypto_symbol(symbol)
                symbol_param = symbol_normalized
                if (
                    symbol_param.endswith("USDT")
                    or symbol_param.endswith("USD")
                    or symbol_param.endswith("BUSD")
                ):
                    pair = symbol_param
                else:
                    pair = f"{symbol_param}USDT"

                if endpoint == "ticker":
                    url = f"{self.BINANCE_URL}/ticker/24hr?symbol={pair}"
                elif endpoint == "klines":
                    url = (
                        f"{self.BINANCE_URL}/klines?symbol={pair}&interval=1h&limit=24"
                    )
                elif endpoint == "trades":
                    url = f"{self.BINANCE_URL}/trades?symbol={pair}&limit=500"
                elif endpoint == "depth":
                    url = f"{self.BINANCE_URL}/depth?symbol={pair}&limit=20"
                else:
                    url = f"{self.BINANCE_URL}/ticker/24hr?symbol={pair}"

                return url, "binance"
                return url, "binance"
            else:
                # yfinance does not use a URL builder in the same way
                return None, "yfinance"

        except Exception as e:
            self.logger.error(f"Error building URL for {symbol}: {str(e)}")
            return "", "unknown"

    def _calculate_backoff(self, attempt: int, base_delay: float = None) -> float:
        if base_delay is None:
            base_delay = self.retry_delay
        
        exponential = base_delay * (2 ** attempt)
        jitter = random.uniform(0, 0.1 * exponential)
        return exponential + jitter

    def _make_request(
        self, url: str, symbol: str, api_type: str, timeout_override: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        attempt = 0
        last_error = None
        response_time_ms = 0
        actual_timeout = timeout_override or self.timeout

        while attempt < self.max_retries:
            try:
                self._enforce_rate_limit(api_type)
                time.sleep(self.rate_limit_delay)

                start_time = time.time()
                response = self.session.get(url, timeout=actual_timeout)
                response_time_ms = int((time.time() - start_time) * 1000)
                
                response.raise_for_status()

                data = response.json()
                
                if not isinstance(data, (dict, list)):
                    raise ValueError(f"Invalid response type: {type(data)}")

                self.logger.debug(
                    f"Fetched {symbol} from {api_type} (attempt {attempt + 1}, {response_time_ms}ms)"
                )
                log_metric(
                    "api_request_success",
                    1,
                    {
                        "symbol": symbol,
                        "api": api_type,
                        "attempt": attempt + 1,
                        "response_time_ms": response_time_ms
                    },
                )
                return data

            except requests.exceptions.Timeout:
                last_error = f"Timeout for {symbol} (>{actual_timeout}s)"
                self.logger.warning(
                    f"{last_error} (attempt {attempt + 1}/{self.max_retries})"
                )
                log_metric(
                    "api_timeout",
                    1,
                    {"symbol": symbol, "api": api_type, "attempt": attempt + 1},
                )

            except requests.exceptions.ConnectionError:
                last_error = f"Connection error for {symbol}"
                self.logger.warning(
                    f"{last_error} (attempt {attempt + 1}/{self.max_retries})"
                )
                log_metric(
                    "api_connection_error",
                    1,
                    {"symbol": symbol, "api": api_type, "attempt": attempt + 1},
                )

            except requests.exceptions.ChunkedEncodingError:
                last_error = f"Chunked encoding error for {symbol}"
                self.logger.warning(
                    f"{last_error} (attempt {attempt + 1}/{self.max_retries})"
                )
                log_metric(
                    "api_chunked_error",
                    1,
                    {"symbol": symbol, "api": api_type, "attempt": attempt + 1},
                )

            except requests.exceptions.HTTPError as http_err:
                resp = getattr(http_err, "response", None)
                status_code = None
                try:
                    status_code = resp.status_code if resp is not None else None
                except Exception:
                    status_code = None

                if status_code == 429:
                    last_error = f"Rate limited (429) for {symbol}"
                    self.logger.warning(
                        f"{last_error} (attempt {attempt + 1}/{self.max_retries})"
                    )
                    log_metric("api_rate_limit", 1, {"symbol": symbol})
                    time.sleep(5)
                elif status_code == 503:
                    last_error = f"Service unavailable (503)"
                    self.logger.warning(
                        f"{last_error} (attempt {attempt + 1}/{self.max_retries})"
                    )
                    log_metric("api_service_unavailable", 1, {"symbol": symbol})
                elif status_code in [500, 502, 504]:
                    last_error = f"Server error ({status_code})"
                    self.logger.warning(
                        f"{last_error} (attempt {attempt + 1}/{self.max_retries})"
                    )
                    log_metric(
                        "api_server_error", 1, {"symbol": symbol, "status": status_code}
                    )
                else:
                    last_error = f"HTTP error {status_code or 'unknown'}"
                    self.logger.warning(last_error)
                    log_metric(
                        "api_http_error", 1, {"symbol": symbol, "status": status_code}
                    )

            except json.JSONDecodeError:
                last_error = f"Invalid JSON for {symbol}"
                self.logger.error(last_error)
                log_metric("api_invalid_json", 1, {"symbol": symbol})
                return None

            except ValueError as e:
                last_error = f"Value error for {symbol}: {str(e)}"
                self.logger.error(last_error)
                log_metric("api_value_error", 1, {"symbol": symbol})
                return None

            except Exception as e:
                last_error = f"Unexpected error for {symbol}: {str(e)}"
                self.logger.error(last_error)
                log_metric("api_unexpected_error", 1, {"symbol": symbol})

            attempt += 1
            if attempt < self.max_retries:
                backoff = self._calculate_backoff(attempt - 1)
                self.logger.debug(f"Retry {symbol} in {backoff:.2f}s...")
                time.sleep(backoff)

        self.logger.error(
            f"Failed {symbol} after {self.max_retries} attempts: {last_error}"
        )
        log_metric("api_max_retries_exceeded", 1, {"symbol": symbol, "last_error": last_error})
        return None

    @correlation_decorator()
    def fetch_single_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if not symbol or not isinstance(symbol, str):
                self.logger.error(f"Invalid symbol: {symbol}")
                return None

            symbol = symbol.upper().strip()

            cache_key = f"{symbol}_cache"
            with self.cache_lock:
                if cache_key in self.cache:
                    cached_data, cached_time = self.cache[cache_key]
                    if time.time() - cached_time < self.cache_ttl:
                        self.logger.debug(f"Cache hit for {symbol}")
                        log_metric("cache_hit", 1, {"symbol": symbol})
                        return cached_data

            asset_type = self._get_asset_type(symbol)
            if asset_type == AssetType.STOCK:
                api_type = "yfinance"
                return self.fetch_stock_data_yfinance(symbol)
            
            # For crypto, use Binance URL builder
            url, api_type = self._build_url(symbol)
            if not url:
                self.logger.error(f"Failed to build URL for {symbol}")
                return None

            data = self._make_request(url, symbol, api_type)

            if data:
                validated_data = self._validate_api_response(data, symbol, api_type)
                if validated_data:
                    if api_type == "binance" and self.fetch_klines:
                        self._fetch_and_add_klines(validated_data, symbol)

                    with self.cache_lock:
                        self.cache[cache_key] = (validated_data, time.time())
                    log_metric("cache_set", 1, {"symbol": symbol})
                return validated_data

            return None

        except Exception as e:
            self.logger.error(f"Error fetching {symbol}: {str(e)}")
            log_metric("fetch_error", 1, {"symbol": symbol})
            return None

    def _fetch_and_add_klines(self, result: Dict[str, Any], symbol: str) -> None:
        try:
            url, api_type = self._build_url(symbol, endpoint="klines")
            if not url:
                self.logger.warning(f"Could not build klines URL for {symbol}")
                return

            klines_data = self._make_request(url, symbol, api_type)
            if klines_data and isinstance(klines_data, list):
                klines = []
                for kline in klines_data:
                    if len(kline) >= 8:
                        klines.append(
                            {
                                "timestamp": kline[0],
                                "open": self._safe_float(kline[1]),
                                "high": self._safe_float(kline[2]),
                                "low": self._safe_float(kline[3]),
                                "close": self._safe_float(kline[4]),
                                "volume": self._safe_float(kline[5], 0.0),
                                "quote_volume": self._safe_float(kline[7], 0.0),
                            }
                        )

                result["klines"] = klines
                result["klines_count"] = len(klines)
                self.logger.debug(f"Added {len(klines)} klines to {symbol}")
                log_metric("klines_added", 1, {"symbol": symbol, "count": len(klines)})
        except Exception as e:
            self.logger.warning(f"Error fetching klines for {symbol}: {str(e)}")

    def _validate_api_response(
        self, data: Dict[str, Any], symbol: str, api_type: str
    ) -> Optional[Dict[str, Any]]:
        try:
            if not isinstance(data, dict):
                self.logger.error(f"Invalid response format for {symbol}")
                return None

            if api_type == "binance":
                return self._validate_binance_response(data, symbol)
            else:
                return None

        except Exception as e:
            self.logger.error(f"Validation error for {symbol}: {str(e)}")
            return None

    def normalize_api_response(
        self, data: Dict[str, Any], symbol: str, api_type: str
    ) -> Optional[Dict[str, Any]]:
        try:
            if not isinstance(data, dict):
                return None

            normalized = {
                "symbol": symbol.upper().strip(),
                "asset_type": "crypto" if self._get_asset_type(symbol) == AssetType.CRYPTO else "stock",
                "source_api": api_type,
                "raw_response": data,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "price": None,
                "volume": None,
                "high": None,
                "low": None,
                "open": None,
                "change": None,
                "change_percent": None,
            }

            if api_type == "binance":
                normalized.update({
                    "price": self._safe_float(data.get("lastPrice")),
                    "volume": self._safe_float(data.get("volume")),
                    "high": self._safe_float(data.get("highPrice")),
                    "low": self._safe_float(data.get("lowPrice")),
                    "open": self._safe_float(data.get("openPrice")),
                    "change": self._safe_float(data.get("priceChange")),
                    "change_percent": self._safe_float(data.get("priceChangePercent")),
                    "bid": self._safe_float(data.get("bidPrice")),
                    "ask": self._safe_float(data.get("askPrice")),
                })

            return normalized

            if normalized["price"] is None or normalized["price"] <= 0:
                self.logger.warning(f"Invalid price for {symbol}: {normalized['price']}")
                return None

            return normalized

        except Exception as e:
            self.logger.error(f"Error normalizing response for {symbol}: {str(e)}")
            return None

    def _extract_percent(self, value: str) -> Optional[float]:
        """Extract percentage value from string (e.g., '1.25%' -> 1.25)"""
        try:
            if not value or not isinstance(value, str):
                return None
            cleaned = value.replace("%", "").strip()
            return self._safe_float(cleaned)
        except Exception:
            return None

    def fetch_stock_data_yfinance(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch stock data using yfinance"""
        try:
            ticker = yf.Ticker(symbol)
            # Get fast info first (often faster than .info)
            info = ticker.fast_info
            
            # Fallback to .info if needed, but fast_info is preferred for price
            try:
                price = info.last_price
                prev_close = info.previous_close
                open_price = info.open
                day_high = info.day_high
                day_low = info.day_low
                volume = info.last_volume
            except:
                # Fallback to full info
                full_info = ticker.info
                price = full_info.get('currentPrice') or full_info.get('regularMarketPrice')
                prev_close = full_info.get('previousClose') or full_info.get('regularMarketPreviousClose')
                open_price = full_info.get('open') or full_info.get('regularMarketOpen')
                day_high = full_info.get('dayHigh') or full_info.get('regularMarketDayHigh')
                day_low = full_info.get('dayLow') or full_info.get('regularMarketDayLow')
                volume = full_info.get('volume') or full_info.get('regularMarketVolume')

            if price is None:
                self.logger.warning(f"No price data found for {symbol} via yfinance")
                return None

            change = price - prev_close if prev_close else 0.0
            change_percent = (change / prev_close) * 100 if prev_close else 0.0

            validated = {
                "symbol": symbol.upper().strip(),
                "asset_type": "stock",
                "price": self._safe_float(price),
                "high": self._safe_float(day_high),
                "low": self._safe_float(day_low),
                "open": self._safe_float(open_price),
                "volume": self._safe_float(volume),
                "change": self._safe_float(change),
                "change_percent": self._safe_float(change_percent),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source_api": "yfinance",
            }
            
            self.logger.debug(f"Fetched {symbol} via yfinance: ${price:.2f}")
            log_metric("api_request_success", 1, {"symbol": symbol, "api": "yfinance"})
            return validated

        except Exception as e:
            self.logger.error(f"yfinance error for {symbol}: {str(e)}")
            log_metric("api_unexpected_error", 1, {"symbol": symbol, "api": "yfinance"})
            return None

    def _validate_binance_response(
        self, data: Dict[str, Any], symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Validate and extract Binance 24hr ticker response"""
        try:
            if not data or not isinstance(data, dict):
                self.logger.warning(f"Invalid Binance response for {symbol}")
                return None

            if "symbol" not in data or not data.get("symbol"):
                self.logger.warning(f"No symbol in Binance response for {symbol}")
                return None

            try:
                price = self._safe_float(data.get("lastPrice", None))
                if price is None or price <= 0:
                    self.logger.warning(f"Invalid price for {symbol}")
                    return None

                symbol_clean = self._normalize_crypto_symbol(symbol)

                validated = {
                    "symbol": symbol_clean.upper().strip(),
                    "asset_type": "crypto",
                    "price": price,
                    "high": self._safe_float(data.get("highPrice", None)),
                    "low": self._safe_float(data.get("lowPrice", None)),
                    "open": self._safe_float(data.get("openPrice", None)),
                    "volume": self._safe_float(data.get("volume", 0)),
                    "change": self._safe_float(data.get("priceChange", None)),
                    "change_percent": self._safe_float(data.get("priceChangePercent", None)),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source_api": "binance",
                }

                self.logger.debug(f"Validated Binance response for {symbol_clean}")
                return validated

            except (ValueError, TypeError) as e:
                self.logger.error(f"Parse error for {symbol}: {str(e)}")
                return None

        except Exception as e:
            self.logger.error(f"Binance validation error for {symbol}: {str(e)}")
            return None

    def fetch_ticker_response(self, symbol: str, api_type: str = "binance") -> Optional[Dict[str, Any]]:
        """Extract ticker/price data from API response"""
        try:
            url, api_type_used = self._build_url(symbol, endpoint="ticker")
            if not url:
                return None
            
            response_data = self._make_request(url, symbol, api_type_used)
            if not response_data or not isinstance(response_data, dict):
                self.logger.warning(f"Invalid ticker response for {symbol}")
                return None
            
            validated = self._validate_api_response(response_data, symbol, api_type_used)
            if not validated:
                self.logger.warning(f"Ticker validation failed for {symbol}")
                return None
            
            self.logger.debug(f"Ticker fetched for {symbol}: ${validated.get('price')}")
            return validated
            
        except Exception as e:
            self.logger.error(f"Error fetching ticker for {symbol}: {str(e)}")
            return None

    def fetch_kline_response(self, symbol: str, interval: str = "1h", limit: int = 100) -> Optional[List[Any]]:
        """Extract kline/candlestick data from Binance"""
        try:
            if self._get_asset_type(symbol) != AssetType.CRYPTO:
                self.logger.debug(f"Klines only for crypto, skipping {symbol}")
                return None
            
            url, api_type = self._build_url(symbol, endpoint="klines")
            if not url:
                return None
            
            url = f"{url}&interval={interval}&limit={limit}"
            
            klines_data = self._make_request(url, symbol, api_type)
            if not isinstance(klines_data, list) or len(klines_data) == 0:
                self.logger.debug(f"No kline data for {symbol}")
                return None
            
            valid_klines = []
            for kline in klines_data:
                if isinstance(kline, (list, tuple)) and len(kline) >= 8:
                    try:
                        float(kline[1])
                        float(kline[2])
                        float(kline[3])
                        float(kline[4])
                        float(kline[7])
                        valid_klines.append(kline)
                    except (ValueError, TypeError, IndexError):
                        continue
            
            self.logger.debug(f"Fetched {len(valid_klines)} klines for {symbol}")
            return valid_klines if valid_klines else None
            
        except Exception as e:
            self.logger.error(f"Error fetching klines for {symbol}: {str(e)}")
            return None

    def fetch_depth_response(self, symbol: str, limit: int = 20) -> Optional[Dict[str, Any]]:
        """Extract order book depth data from Binance"""
        try:
            if self._get_asset_type(symbol) != AssetType.CRYPTO:
                self.logger.debug(f"Depth only for crypto, skipping {symbol}")
                return None
            
            url, api_type = self._build_url(symbol, endpoint="depth")
            if not url:
                return None
            
            url = f"{url}&limit={limit}"
            
            depth_data = self._make_request(url, symbol, api_type)
            if not isinstance(depth_data, dict):
                self.logger.debug(f"Invalid depth response for {symbol}")
                return None
            
            if "bids" not in depth_data or "asks" not in depth_data:
                self.logger.warning(f"Missing bids/asks for {symbol}")
                return None
            
            bids = depth_data.get("bids", [])
            asks = depth_data.get("asks", [])
            
            if not isinstance(bids, list) or not isinstance(asks, list):
                return None
            
            depth_metrics = {
                "symbol": symbol,
                "timestamp": datetime.utcnow().isoformat(),
                "bid_count": len(bids),
                "ask_count": len(asks),
                "bid_volume": sum(self._safe_float(b[1], 0) for b in bids if len(b) >= 2),
                "ask_volume": sum(self._safe_float(a[1], 0) for a in asks if len(a) >= 2),
                "spread": None
            }
            
            if len(bids) > 0 and len(asks) > 0:
                best_bid = self._safe_float(bids[0][0], 0)
                best_ask = self._safe_float(asks[0][0], 0)
                if best_bid > 0 and best_ask > 0:
                    depth_metrics["spread"] = best_ask - best_bid
            
            self.logger.debug(f"Depth fetched for {symbol}: {depth_metrics['bid_count']} bids, {depth_metrics['ask_count']} asks")
            return depth_metrics
            
        except Exception as e:
            self.logger.error(f"Error fetching depth for {symbol}: {str(e)}")
            return None

    def fetch_all_symbols(self, max_workers: int = 5, use_parallel: bool = True) -> Dict[str, Any]:
        results = {}
        start_time = time.time()
        successful_fetches = 0
        failed_fetches = 0

        try:
            symbols_to_fetch = list(dict.fromkeys(self.symbols + self.fallback_symbols))

            if not symbols_to_fetch:
                self.logger.warning("No symbols to fetch")
                return results

            self.logger.info(
                f"Starting fetch for {len(symbols_to_fetch)} symbols "
                f"(parallel={use_parallel}, workers={max_workers})"
            )

            # Fetch all in parallel (yfinance handles concurrency well)
            if use_parallel and len(symbols_to_fetch) > 1:
                results, successful_fetches, failed_fetches = self._fetch_parallel(
                    symbols_to_fetch, max_workers
                )
            else:
                results, successful_fetches, failed_fetches = self._fetch_sequential(
                    symbols_to_fetch
                )

            elapsed_time = time.time() - start_time
            success_rate = (
                round((successful_fetches / len(symbols_to_fetch)) * 100, 2)
                if symbols_to_fetch
                else 0
            )

            self.logger.info(
                f"Fetch complete: {successful_fetches}/{len(symbols_to_fetch)} ({success_rate}%) in {elapsed_time:.2f}s"
            )
            log_metric(
                "fetch_all_complete",
                1,
                {
                    "successful": successful_fetches,
                    "failed": failed_fetches,
                    "total": len(symbols_to_fetch),
                    "duration_seconds": elapsed_time,
                    "success_rate": success_rate,
                    "parallel": use_parallel,
                },
            )

            return results

        except Exception as e:
            self.logger.error(f"Error in fetch_all_symbols: {str(e)}")
            log_metric("fetch_all_error", 0, {"error": str(e)})
            return results

    def _fetch_sequential(
        self, symbols_to_fetch: List[str]
    ) -> Tuple[Dict[str, Any], int, int]:
        results = {}
        successful_fetches = 0
        failed_fetches = 0

        for i, symbol in enumerate(symbols_to_fetch, 1):
            try:
                self.logger.debug(f"Fetching {i}/{len(symbols_to_fetch)}: {symbol}")
                data = self.fetch_single_symbol(symbol)
                if data:
                    results[symbol] = data
                    successful_fetches += 1
                else:
                    failed_fetches += 1
            except Exception as e:
                failed_fetches += 1
                self.logger.error(f"Error fetching {symbol}: {str(e)}")

        return results, successful_fetches, failed_fetches

    def _fetch_parallel(
        self, symbols_to_fetch: List[str], max_workers: int
    ) -> Tuple[Dict[str, Any], int, int]:
        results = {}
        successful_fetches = 0
        failed_fetches = 0

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_symbol = {
                    executor.submit(self.fetch_single_symbol, symbol): symbol
                    for symbol in symbols_to_fetch
                }

                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        data = future.result(timeout=self.timeout + 5)
                        if data:
                            results[symbol] = data
                            successful_fetches += 1
                        else:
                            failed_fetches += 1
                    except Exception as e:
                        failed_fetches += 1
                        self.logger.error(
                            f"Error fetching {symbol} in parallel: {str(e)}"
                        )

        except Exception as e:
            self.logger.error(f"Error in parallel execution: {str(e)}")

        return results, successful_fetches, failed_fetches

    def _is_cache_valid(self, symbol: str, custom_ttl: Optional[int] = None) -> bool:
        """Check if cached data for symbol is still valid (per-symbol TTL)"""
        try:
            with self.cache_lock:
                if symbol not in self.cache:
                    return False
                
                ttl = custom_ttl or self.cache_ttl
                timestamp = self.cache_timestamps.get(symbol, 0)
                current_time = time.time()
                
                is_valid = (current_time - timestamp) < ttl
                if not is_valid:
                    del self.cache[symbol]
                    del self.cache_timestamps[symbol]
                
                return is_valid
        except Exception as e:
            self.logger.debug(f"Cache validation error for {symbol}: {str(e)}")
            return False

    def _get_cached(self, symbol: str, custom_ttl: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Get data from cache if valid"""
        if self._is_cache_valid(symbol, custom_ttl):
            try:
                with self.cache_lock:
                    return self.cache.get(symbol)
            except Exception as e:
                self.logger.debug(f"Error retrieving cache for {symbol}: {str(e)}")
        return None

    def _set_cache(self, symbol: str, data: Dict[str, Any]) -> None:
        """Set cache with timestamp for per-symbol TTL tracking"""
        try:
            with self.cache_lock:
                self.cache[symbol] = data
                self.cache_timestamps[symbol] = time.time()
        except Exception as e:
            self.logger.debug(f"Error setting cache for {symbol}: {str(e)}")

    def save_raw(self, data: Dict[str, Any]) -> bool:
        try:
            if not data:
                self.logger.warning("No data to save (raw)")
                return False

            raw_dir = Path(
                self.config.get("data_sources", {}).get("raw_data_path", "data/raw")
            )
            raw_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            file_path = raw_dir / f"raw_data_{timestamp}.json"

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Raw data saved: {file_path} ({len(data)} records)")
            log_metric(
                "save_raw_success",
                1,
                {"records": len(data), "size_bytes": file_path.stat().st_size},
            )
            return True

        except IOError as e:
            self.logger.error(f"IO error saving raw data: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error saving raw data: {str(e)}")
            return False

    def save_processed(self, data: Dict[str, Any]) -> bool:
        try:
            if not data:
                self.logger.warning("No data to save (processed)")
                return False

            processed_dir = Path(
                self.config.get("data_sources", {}).get(
                    "processed_data_path", "data/processed"
                )
            )
            processed_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            file_path = processed_dir / f"processed_data_{timestamp}.json"

            processed_data = {}
            for symbol, item in data.items():
                try:
                    processed_data[symbol] = {
                        "symbol": item.get("symbol", symbol),
                        "type": item.get("type", "unknown"),
                        "price": item.get("price"),
                        "high": item.get("high"),
                        "low": item.get("low"),
                        "open": item.get("open"),
                        "volume": item.get("volume"),
                        "change": item.get("change"),
                        "change_percent": item.get("change_percent"),
                        "timestamp": item.get("timestamp"),
                        "api": item.get("api"),
                        "data_format": item.get("data_format", "ml_ready_v1"),
                        "klines": item.get("klines"),
                        "klines_count": item.get("klines_count"),
                    }
                except Exception as e:
                    self.logger.warning(f"Error processing {symbol}: {str(e)}")

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(processed_data, f, indent=2, ensure_ascii=False)

            self.logger.info(
                f"Processed data saved: {file_path} ({len(processed_data)} records)"
            )
            log_metric(
                "save_processed_success",
                1,
                {
                    "records": len(processed_data),
                    "size_bytes": file_path.stat().st_size,
                },
            )
            return True

        except IOError as e:
            self.logger.error(f"IO error saving processed data: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error saving processed data: {str(e)}")
            return False

    def run_once(self) -> Optional[Dict[str, Any]]:
        try:
            self.logger.info("Starting single run")
            log_metric("run_once_start", 1, {})

            results = self.fetch_all_symbols()

            if results:
                self.save_raw(results)
                self.save_processed(results)
                self.logger.info(f"Run complete: {len(results)} records")
                log_metric("run_once_success", 1, {"records": len(results)})
                return results
            else:
                self.logger.warning("Run complete but no data fetched")
                log_metric("run_once_no_data", 1, {})
                return None

        except Exception as e:
            self.logger.error(f"Error in run_once: {str(e)}")
            log_metric("run_once_error", 0, {"error": str(e)})
            return None

    def clear_cache(self) -> None:
        try:
            with self.cache_lock:
                self.cache.clear()
            self.logger.info("Cache cleared")
            log_metric("cache_cleared", 1, {})
        except Exception as e:
            self.logger.error(f"Error clearing cache: {str(e)}")

    def get_cache_stats(self) -> Dict[str, Any]:
        try:
            with self.cache_lock:
                cache_items = list(self.cache.keys())
                cache_size = len(cache_items)

            stats = {
                "cache_size": cache_size,
                "cache_items": cache_items,
                "cache_ttl": self.cache_ttl,
            }
            self.logger.debug(f"Cache stats: {stats}")
            return stats
        except Exception as e:
            self.logger.error(f"Error getting cache stats: {str(e)}")
            return {}

    def health_check(self) -> Dict[str, Any]:
        try:
            health = {
                "status": "healthy",
                "session_active": self.session is not None,
                "config_loaded": self.config is not None,
                "symbols_count": len(self.symbols),
                "timeout": self.timeout,
                "timestamp": datetime.utcnow().isoformat(),
                "rate_limit_lock": "thread_safe",
            }
            self.logger.debug(f"Health check: OK")
            log_metric("health_check_success", 1, health)
            return health
        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}")
            log_metric("health_check_error", 0, {"error": str(e)})
            return {"status": "unhealthy", "error": str(e)}

    def __del__(self):
        try:
            if self.session:
                self.session.close()
                self.logger.debug("HTTP session closed")
        except Exception as e:
            self.logger.error(f"Error closing session: {str(e)}")
