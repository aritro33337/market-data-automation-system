import json
from pathlib import Path
from typing import Any, Dict, Optional


class ConfigLoader:

    @staticmethod
    def _get_logger():
        from .logger import get_logger
        try:
            return get_logger("ConfigLoader")
        except:
            import logging
            return logging.getLogger("ConfigLoader")

    @staticmethod
    def _log_metric(name: str, value: Any, tags: Dict[str, Any]):
        try:
            from .logger import log_metric
            log_metric(name, value, tags)
        except:
            pass

    @staticmethod
    def load(path: str) -> Dict[str, Any]:
        logger = ConfigLoader._get_logger()
        path = Path(path)

        if not path.exists():
            msg = f"Config file not found: {path}"
            logger.error(msg)
            ConfigLoader._log_metric(
                "config_load_failure",
                0,
                {"path": str(path), "reason": "file_not_found"}
            )
            raise FileNotFoundError(msg)

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            logger.info(f"Loaded config file successfully: {path}")
            ConfigLoader._log_metric(
                "config_load_success",
                1,
                {"path": str(path)}
            )
            return data

        except json.JSONDecodeError as e:
            msg = f"JSON decode error in {path}: {e}"
            logger.error(msg)
            ConfigLoader._log_metric(
                "config_load_failure",
                0,
                {"path": str(path), "reason": "json_decode_error"}
            )
            raise

        except Exception as e:
            msg = f"Unexpected error loading config file {path}: {e}"
            logger.error(msg)
            ConfigLoader._log_metric(
                "config_load_failure",
                0,
                {"path": str(path), "reason": "unexpected_error"}
            )
            raise


    @staticmethod
    def load_config(path: str) -> Optional[Dict[str, Any]]:
        logger = ConfigLoader._get_logger()
        path = Path(path)

        if not path.exists():
            logger.error(f"[ConfigLoader] settings.json missing: {path}")
            ConfigLoader._log_metric(
                "settings_json_missing",
                0,
                {"path": str(path)}
            )
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                config = json.load(f)

            logger.info(f"[ConfigLoader] settings.json loaded: {path}")
            ConfigLoader._log_metric(
                "settings_json_loaded",
                1,
                {"path": str(path)}
            )
            return config

        except Exception as e:
            logger.error(f"[ConfigLoader] Failed to load settings.json: {e}")
            ConfigLoader._log_metric(
                "settings_json_failure",
                0,
                {"path": str(path), "reason": str(e)}
            )
            return None
