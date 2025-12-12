import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from datetime import datetime, timezone
import os
import threading
import time
import csv
from src.utils.logger import get_logger, log_metric, correlation_decorator
from src.utils.run_id_manager import RunIdManager

class CsvExporter:
    def __init__(self, export_dir: str = "data/exports", delimiter: str = ",", include_header: bool = True):
        self.logger = get_logger("CsvExporter")
        self.export_dir = Path(export_dir)
        self.delimiter = delimiter
        self.include_header = include_header
        self.lock = threading.RLock()
        
        try:
            self._ensure_export_directory()
            self.logger.info(
                f"CsvExporter initialized (export_dir={str(self.export_dir)}, "
                f"delimiter='{self.delimiter}', header={self.include_header})"
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize CsvExporter: {str(e)}")
            raise

    def _ensure_export_directory(self) -> None:
        try:
            self.export_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            self.logger.error(f"Failed to create export directory: {str(e)}")
            raise

    def _to_dataframe(self, data: Any) -> pd.DataFrame:
        try:
            if isinstance(data, pd.DataFrame):
                return data.copy()

            if isinstance(data, dict):
                if not data:
                    return pd.DataFrame()
                if isinstance(next(iter(data.values()), None), (dict, list)):
                    df = pd.DataFrame.from_dict(data, orient='index')
                    df = df.reset_index()
                    return df
                else:
                    return pd.DataFrame([data])

            if isinstance(data, list):
                if not data:
                    return pd.DataFrame()
                return pd.DataFrame(data)

            self.logger.warning(f"Unsupported data format: {type(data)}")
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error converting data to DataFrame: {str(e)}")
            return pd.DataFrame()

    def _get_next_run_id(self) -> int:
        """Get the next run ID using the centralized manager"""
        return RunIdManager().get_next_run_id(increment=True)

    @correlation_decorator()
    def export(self, data: Any, filename_prefix: str = "market_data", run_id: Optional[int] = None) -> Optional[str]:
        try:
            if data is None:
                self.logger.warning("No data provided for CSV export")
                return None

            df = self._to_dataframe(data)
            
            if df.empty:
                self.logger.warning("DataFrame is empty, skipping CSV export")
                return None
                for col in df.columns:
                    df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)

            timestamp = datetime.now(timezone.utc).strftime("%d-%m-%Y_%H-%M-%S")
            
            if run_id is None:
                run_id = self._get_next_run_id()
                
            filename = f"{filename_prefix}_{timestamp}_{run_id}.csv"
            filepath = self.export_dir / filename

            with self.lock:
                df.to_csv(
                    filepath, 
                    index=False, 
                    sep=self.delimiter, 
                    header=self.include_header, 
                    encoding='utf-8',
                    quoting=csv.QUOTE_MINIMAL
                )
                
            self.logger.info(f"CSV file saved: {str(filepath)}")
            log_metric("csv_export_success", 1, {"filename": filename})
            return str(filepath)

        except Exception as e:
            self.logger.error(f"Error during CSV export: {str(e)}")
            log_metric("csv_export_error", 0, {"error": str(e)})
            return None
