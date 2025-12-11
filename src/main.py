import sys
import signal
import json
import os
import uuid
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import time
import threading
from contextlib import contextmanager
import traceback
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("WARNING: 'python-dotenv' not found. .env file will not be loaded.")
    print("Make sure you have installed 'python-dotenv' in your environment.")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.live_data_fetcher import LiveDataFetcher
from src.core.aggregator import DataAggregator
from src.core.excel_exporter import ExcelExporter
from src.core.scheduler import Scheduler, JobType
from src.ml_inference.inference import MLInferenceEngine
from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader


class PipelineOrchestrator:
    def __init__(self, config_path: str = "config/settings.json"):
        self.logger = get_logger("PipelineOrchestrator")
        self.correlation_id = str(uuid.uuid4())[:8]
        self.config = None
        self.fetcher = None
        self.aggregator = None
        self.exporter = None
        self.ml_engine = None
        self.scheduler = None
        self.shutdown_event = False
        
        try:
            self._initialize_system(config_path)
            self.logger.info(f"PipelineOrchestrator initialized successfully [CID:{self.correlation_id}]")
        except Exception as e:
            self.logger.error(f"Failed to initialize PipelineOrchestrator [CID:{self.correlation_id}]: {str(e)}")
            raise
    
    def _initialize_system(self, config_path: str) -> None:
        try:
            config_path = Path(config_path)
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")
            
            self.config = ConfigLoader.load(str(config_path))
            if not self.config:
                raise ValueError("Configuration loaded but empty")
            
            self.config["__config_path__"] = str(config_path)
            self.config["__correlation_id__"] = self.correlation_id
            
            self._initialize_directories()
            self._initialize_core_modules()
            self._initialize_ml_system()
            self._initialize_scheduler()
            
            self.logger.info(f"All system components initialized [CID:{self.correlation_id}]")
        except Exception as e:
            self.logger.error(f"System initialization failed [CID:{self.correlation_id}]: {str(e)}")
            raise
    
    
    def _initialize_directories(self) -> None:
        try:
            directories = {
                "raw": self.config.get("data_sources", {}).get("raw_data_path", "data/raw/"),
                "processed": self.config.get("data_sources", {}).get("processed_data_path", "data/processed/"),
                "exports": self.config.get("export", {}).get("excel_output_folder", "data/exports/"),
                "ml_ready": "data/ml_ready/",
                "logs": self.config.get("logging", {}).get("log_dir", "logs/")
            }
            
            for dir_name, dir_path in directories.items():
                path = Path(dir_path)
                if not path.exists():
                    path.mkdir(parents=True, exist_ok=True)
                    self.logger.info(f"Created directory: {dir_path} [CID:{self.correlation_id}]")
        except Exception as e:
            self.logger.error(f"Directory initialization failed [CID:{self.correlation_id}]: {str(e)}")
            raise
    
    def _initialize_core_modules(self) -> None:
        try:
            config_path = self.config.get("__config_path__", "config/settings.json")
            
            self.fetcher = LiveDataFetcher(config_path)
            self.logger.info(f"LiveDataFetcher initialized [CID:{self.correlation_id}]")
            
            self.aggregator = DataAggregator(config_path)
            self.logger.info(f"DataAggregator initialized [CID:{self.correlation_id}]")
            
            export_dir = self.config.get("export", {}).get("excel_output_folder", "data/exports/")
            self.exporter = ExcelExporter(export_dir=export_dir)
            self.logger.info(f"ExcelExporter initialized [CID:{self.correlation_id}]")
        except Exception as e:
            self.logger.error(f"Core modules initialization failed [CID:{self.correlation_id}]: {str(e)}")
            raise
    
    def _initialize_ml_system(self) -> None:
        try:
            ml_config = self.config.get("ml", {})
            ml_enabled = ml_config.get("inference_enabled", False)
            
            if not ml_enabled:
                self.logger.info(f"ML inference disabled in config [CID:{self.correlation_id}]")
                self.ml_engine = None
                return
            
            model_path = ml_config.get("model_path", "ml/models/latest_model.pkl")
            feature_config_path = ml_config.get("feature_config_path", "ml/features/feature_config.json")
            
            try:
                if not Path(model_path).exists():
                    self.logger.warning(f"Model file not found: {model_path} [CID:{self.correlation_id}]")
                    self.ml_engine = None
                    return
                
                if not Path(feature_config_path).exists():
                    self.logger.warning(f"Feature config not found: {feature_config_path} [CID:{self.correlation_id}]")
                    self.ml_engine = None
                    return
                
                self.ml_engine = MLInferenceEngine(model_path, feature_config_path)
                self.logger.info(f"MLInferenceEngine initialized successfully [CID:{self.correlation_id}]")
            except Exception as ml_error:
                self.logger.warning(f"ML initialization failed, continuing without ML [CID:{self.correlation_id}]: {str(ml_error)}")
                self.ml_engine = None
        except Exception as e:
            self.logger.error(f"ML system initialization failed [CID:{self.correlation_id}]: {str(e)}")
            self.ml_engine = None
    
    def _initialize_scheduler(self) -> None:
        try:
            self.scheduler = Scheduler()
            self.logger.info(f"Scheduler initialized [CID:{self.correlation_id}]")
        except Exception as e:
            self.logger.error(f"Scheduler initialization failed [CID:{self.correlation_id}]: {str(e)}")
            raise
    
    def _fetch_data(self) -> Optional[Dict[str, Any]]:
        try:
            if not self.fetcher:
                self.logger.error(f"Fetcher not initialized [CID:{self.correlation_id}]")
                return None
            
            raw_data = self.fetcher.fetch_all_symbols()
            if not raw_data:
                self.logger.warning(f"Fetcher returned no data [CID:{self.correlation_id}]")
                return None
            
            self.logger.info(f"Fetched data for {len(raw_data)} symbols [CID:{self.correlation_id}]")
            return raw_data
        except Exception as e:
            self.logger.error(f"Data fetch failed [CID:{self.correlation_id}]: {str(e)}")
            return None
    
    def _aggregate_data(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            if not raw_data:
                self.logger.warning(f"No raw data to aggregate [CID:{self.correlation_id}]")
                return None
            
            if not self.aggregator:
                self.logger.error(f"Aggregator not initialized [CID:{self.correlation_id}]")
                return None
            
            aggregated = self.aggregator.run(raw_data)
            if not aggregated:
                self.logger.warning(f"Aggregation returned no data [CID:{self.correlation_id}]")
                return None
            
            self.logger.info(f"Aggregated {len(aggregated.get('symbols', {}))} symbols [CID:{self.correlation_id}]")
            return aggregated
        except Exception as e:
            self.logger.error(f"Data aggregation failed [CID:{self.correlation_id}]: {str(e)}")
            return None
    
    def _apply_ml_predictions(self, aggregated_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not self.ml_engine:
                self.logger.debug(f"ML inference disabled, skipping [CID:{self.correlation_id}]")
                return aggregated_data
            
            if not aggregated_data or "symbols" not in aggregated_data:
                self.logger.warning(f"Invalid aggregated data for ML inference [CID:{self.correlation_id}]")
                return aggregated_data
            
            try:
                predictions = self.ml_engine.predict(aggregated_data)
                if predictions:
                    aggregated_data["ml_predictions"] = predictions
                    self.logger.info(f"ML predictions added: {len(predictions)} items [CID:{self.correlation_id}]")
            except Exception as ml_error:
                self.logger.warning(f"ML prediction failed, continuing without predictions [CID:{self.correlation_id}]: {str(ml_error)}")
            
            return aggregated_data
        except Exception as e:
            self.logger.error(f"ML inference step failed [CID:{self.correlation_id}]: {str(e)}")
            return aggregated_data
    
    def _export_data(self, processed_data: Dict[str, Any]) -> bool:
        try:
            if not processed_data or "symbols" not in processed_data:
                self.logger.warning(f"No valid data to export [CID:{self.correlation_id}]")
                return False
            
            if not self.exporter:
                self.logger.error(f"Exporter not initialized [CID:{self.correlation_id}]")
                return False
            
            filename_prefix = "market_data"
            
            # 1. Save Excel-ready JSON (legacy support + backup)
            excel_rows = processed_data.get("excel_rows")
            if excel_rows:
                try:
                    output_dir = "data/exports/"
                    Path(output_dir).mkdir(parents=True, exist_ok=True)
                    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                    json_filename = f"market_data_{timestamp}.json"
                    json_filepath = Path(output_dir) / json_filename
                    
                    with open(json_filepath, "w", encoding="utf-8") as f:
                        json.dump(excel_rows, f, indent=2, ensure_ascii=False)
                    self.logger.info(f"Saved Excel-ready JSON: {json_filepath}")
                except Exception as e:
                    self.logger.error(f"Failed to save Excel-ready JSON: {e}")

            # 2. Save ML-ready JSON
            ml_data = processed_data.get("ml_data")
            if ml_data:
                try:
                    ml_dir = "data/ml_ready/"
                    Path(ml_dir).mkdir(parents=True, exist_ok=True)
                    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                    ml_filename = f"ml_ready_{timestamp}.json"
                    ml_filepath = Path(ml_dir) / ml_filename
                    
                    with open(ml_filepath, "w", encoding="utf-8") as f:
                        json.dump(ml_data, f, indent=2)
                    self.logger.info(f"Saved ML-ready JSON: {ml_filepath}")
                except Exception as e:
                    self.logger.error(f"Failed to save ML-ready JSON: {e}")

            # 3. Export to Excel (.xlsx)
            data_to_export = excel_rows if excel_rows else processed_data
            success = self.exporter.export_to_excel(data_to_export, filename_prefix)
            
            if success:
                self.logger.info(f"Data exported successfully: {success} [CID:{self.correlation_id}]")
            else:
                self.logger.error(f"Data export failed [CID:{self.correlation_id}]")

            return bool(success)
        except Exception as e:
            self.logger.error(f"Data export failed [CID:{self.correlation_id}]: {str(e)}")
            return False
    
    def run_full_pipeline(self) -> bool:
        try:
            self.logger.info(f"Starting full pipeline execution [CID:{self.correlation_id}]")
            
            raw_data = self._fetch_data()
            if not raw_data:
                self.logger.error(f"Pipeline failed at fetch stage [CID:{self.correlation_id}]")
                return False
            
            aggregated_data = self._aggregate_data(raw_data)
            if not aggregated_data:
                self.logger.error(f"Pipeline failed at aggregation stage [CID:{self.correlation_id}]")
                return False
            
            processed_data = self._apply_ml_predictions(aggregated_data)
            
            export_success = self._export_data(processed_data)
            if not export_success:
                self.logger.error(f"Pipeline failed at export stage [CID:{self.correlation_id}]")
                return False
            
            self.logger.info(f"Pipeline execution completed successfully [CID:{self.correlation_id}]")
            return True
        except Exception as e:
            self.logger.error(f"Full pipeline execution failed [CID:{self.correlation_id}]: {str(e)}")
            return False
    
    def register_pipeline_with_scheduler(self) -> None:
        try:
            scheduler_config = self.config.get("scheduler", {})
            if not scheduler_config.get("enabled", True):
                self.logger.warning(f"Scheduler disabled in config [CID:{self.correlation_id}]")
                return
            
            interval = scheduler_config.get("interval_seconds", 300)
            if interval <= 0:
                interval = 300
            
            self.scheduler.register_job(
                name="market_data_pipeline",
                func=self.run_full_pipeline,
                job_type=JobType.INTERVAL,
                interval_seconds=interval,
                enabled=True,
                max_retries=3,
                retry_delay_seconds=5.0,
                timeout_seconds=300.0
            )
            self.logger.info(f"Pipeline registered with scheduler (interval={interval}s) [CID:{self.correlation_id}]")
        except Exception as e:
            self.logger.error(f"Failed to register pipeline with scheduler [CID:{self.correlation_id}]: {str(e)}")
            raise
    
    def _handle_shutdown_signal(self, signum, frame):
        self.logger.info(f"Shutdown signal received (signal={signum}) [CID:{self.correlation_id}]")
        self.shutdown_event = True
        if self.scheduler:
            try:
                self.scheduler.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping scheduler during signal handling [CID:{self.correlation_id}]: {str(e)}")
    
    def start(self) -> None:
        try:
            signal.signal(signal.SIGINT, self._handle_shutdown_signal)
            signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
            
            self.logger.info("=" * 60)
            self.logger.info("Market Data Automation System - ENTERPRISE MODE")
            self.logger.info(f"Start Time: {datetime.now(timezone.utc).isoformat()}")
            self.logger.info(f"Correlation ID: {self.correlation_id}")
            self.logger.info("=" * 60)
            
            self.register_pipeline_with_scheduler()
            
            scheduler_config = self.config.get("scheduler", {})
            if not scheduler_config.get("enabled", True):
                self.logger.info(f"Scheduler disabled, running pipeline once [CID:{self.correlation_id}]")
                self.run_full_pipeline()
                return

            if self.scheduler:
                self.logger.info(f"Starting scheduler [CID:{self.correlation_id}]...")
                self.scheduler.start()
                
                try:
                    while not self.shutdown_event:
                        import time
                        time.sleep(1)
                except KeyboardInterrupt:
                    self.logger.info(f"Keyboard interrupt received [CID:{self.correlation_id}]")
            else:
                self.logger.error(f"Scheduler failed to start [CID:{self.correlation_id}]")
        except Exception as e:
            self.logger.error(f"Startup failed [CID:{self.correlation_id}]: {str(e)}")
            raise
        finally:
            self.shutdown()
    
    def shutdown(self) -> None:
        try:
            self.logger.info(f"Initiating graceful shutdown [CID:{self.correlation_id}]...")
            
            if self.scheduler:
                try:
                    self.scheduler.stop()
                    self.logger.info(f"Scheduler stopped [CID:{self.correlation_id}]")
                except Exception as e:
                    self.logger.warning(f"Error stopping scheduler [CID:{self.correlation_id}]: {str(e)}")
            
            self.logger.info("=" * 60)
            self.logger.info(f"Shutdown Time: {datetime.now(timezone.utc).isoformat()}")
            self.logger.info(f"Correlation ID: {self.correlation_id}")
            self.logger.info("System shutdown complete")
            self.logger.info("=" * 60)
        except Exception as e:
            self.logger.error(f"Shutdown error [CID:{self.correlation_id}]: {str(e)}")


def main() -> int:
    try:
        config_path = os.environ.get("CONFIG_PATH", "config/settings.json")
        orchestrator = PipelineOrchestrator(config_path)
        orchestrator.start()
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"Fatal error: {str(e)}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

