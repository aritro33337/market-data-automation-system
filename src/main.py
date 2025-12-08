import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from src.core.aggregator import DataAggregator
from src.core.live_data_fetcher import LiveDataFetcher
from src.utils.logger import get_logger


logger = get_logger("CLI")


def load_json_input(path: str) -> Dict[str, Any]:
	p = Path(path)
	if not p.exists():
		logger.error(f"Input file not found: {path}")
		raise FileNotFoundError(path)

	with open(p, "r", encoding="utf-8") as f:
		data = json.load(f)
	if isinstance(data, dict):
		return data

	if isinstance(data, list):
		out = {}
		for item in data:
			if isinstance(item, dict) and "symbol" in item:
				out[item["symbol"]] = item
		return out

	raise ValueError("Unsupported input JSON format")


def cmd_run(args: argparse.Namespace) -> int:
	try:
		cfg_path = args.config or "config/settings.json"
		aggregator = DataAggregator(cfg_path)

		if args.source == "live":
			fetcher = LiveDataFetcher(cfg_path)
			raw = fetcher.fetch_all_symbols()
			if not raw:
				logger.error("Live fetch returned no data")
				return 2
		else:
			if not args.file:
				logger.error("--file is required when --source file")
				return 2
			raw = load_json_input(args.file)

		aggregated = aggregator.run(raw)
		if aggregated is None:
			logger.error("Aggregation failed")
			return 3
		if getattr(args, "save_processed", False):
			processed_path = Path(aggregator.config.get("data_sources", {}).get("processed_data_path", "data/processed/"))
			processed_path.mkdir(parents=True, exist_ok=True)
			out_file = processed_path / f"processed_cli_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
			with open(out_file, "w", encoding="utf-8") as f:
				json.dump(aggregated, f, indent=2, ensure_ascii=False)
			logger.info(f"Saved processed snapshot: {out_file}")

		print(json.dumps({"status": "ok", "symbols": len(aggregated.get("symbols", {}))}, ensure_ascii=False))
		return 0

	except Exception as e:
		logger.error(f"CLI run failed: {e}")
		return 1


def cmd_status(args: argparse.Namespace) -> int:
	try:
		aggregator = DataAggregator(args.config or "config/settings.json")
		status = aggregator.get_cache_status()
		print(json.dumps(status, indent=2, ensure_ascii=False))
		return 0
	except Exception as e:
		logger.error(f"CLI status failed: {e}")
		return 1


def cmd_clear_cache(args: argparse.Namespace) -> int:
	try:
		aggregator = DataAggregator(args.config or "config/settings.json")
		ok = aggregator.clear_cache()
		print(json.dumps({"cleared": ok}))
		return 0 if ok else 2
	except Exception as e:
		logger.error(f"CLI clear-cache failed: {e}")
		return 1


def main(argv=None) -> int:
	parser = argparse.ArgumentParser("Market Data Automation CLI")
	parser.add_argument("--config", help="config path", default="config/settings.json")

	sub = parser.add_subparsers(dest="command")

	p_run = sub.add_parser("run", help="Run aggregation")
	p_run.add_argument("--source", choices=["file", "live"], default="file")
	p_run.add_argument("--file", help="Input JSON file (for file source)")
	p_run.add_argument("--save-processed", action="store_true", help="Save processed aggregated snapshot")

	p_status = sub.add_parser("status", help="Show aggregator cache status")

	p_clear = sub.add_parser("clear-cache", help="Clear aggregator caches")

	args = parser.parse_args(argv)

	if args.command == "run":
		return cmd_run(args)
	if args.command == "status":
		return cmd_status(args)
	if args.command == "clear-cache":
		return cmd_clear_cache(args)

	parser.print_help()
	return 0


if __name__ == "__main__":
	raise SystemExit(main())

