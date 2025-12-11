import pytest
from src.core.excel_exporter import ExcelExporter, MLExcelHelper, CurrencySymbol
from pathlib import Path
import pandas as pd
import tempfile
import os


class TestExcelExporter:

    @pytest.fixture
    def temp_export_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def exporter(self, temp_export_dir):
        return ExcelExporter(export_dir=temp_export_dir, currency="USD")

    def test_initialization(self, exporter):
        assert exporter is not None
        assert exporter.export_dir is not None
        assert exporter.currency == "$"
        assert exporter.append_mode is False

    def test_currency_usd(self):
        exporter = ExcelExporter(currency="USD")
        assert exporter.currency == "$"

    def test_currency_eur(self):
        exporter = ExcelExporter(currency="EUR")
        assert exporter.currency == "€"

    def test_currency_gbp(self):
        exporter = ExcelExporter(currency="GBP")
        assert exporter.currency == "£"

    def test_currency_invalid(self):
        exporter = ExcelExporter(currency="INVALID")
        assert exporter.currency == "$"

    def test_set_template_valid(self, exporter, temp_export_dir):
        template_path = os.path.join(temp_export_dir, "template.xlsx")
        Path(template_path).touch()
        result = exporter.set_template(template_path)
        assert result is True
        assert exporter.template_path == template_path

    def test_set_template_invalid(self, exporter):
        result = exporter.set_template("/non/existent/path.xlsx")
        assert result is False

    def test_enable_append_mode_true(self, exporter):
        exporter.enable_append_mode(True)
        assert exporter.append_mode is True

    def test_enable_append_mode_false(self, exporter):
        exporter.enable_append_mode(False)
        assert exporter.append_mode is False

    def test_to_dataframe_from_dict(self, exporter):
        data = {
            "symbol": "AAPL",
            "price": 150.0,
            "volume": 1000000
        }
        df = exporter._to_dataframe(data)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_to_dataframe_from_list(self, exporter):
        data = [
            {"symbol": "AAPL", "price": 150.0},
            {"symbol": "MSFT", "price": 370.0}
        ]
        df = exporter._to_dataframe(data)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_to_dataframe_from_dataframe(self, exporter):
        original_df = pd.DataFrame({
            "symbol": ["AAPL", "MSFT"],
            "price": [150.0, 370.0]
        })
        df = exporter._to_dataframe(original_df)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_to_dataframe_invalid(self, exporter):
        df = exporter._to_dataframe("invalid")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_validate_dataframe_valid(self, exporter):
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "price": [150.0]
        })
        assert exporter._validate_dataframe(df) is True

    def test_validate_dataframe_empty(self, exporter):
        df = pd.DataFrame()
        assert exporter._validate_dataframe(df) is True

    def test_validate_dataframe_none(self, exporter):
        assert exporter._validate_dataframe(None) is False

    def test_validate_dataframe_string(self, exporter):
        assert exporter._validate_dataframe("invalid") is False

    def test_clean_column_names(self, exporter):
        columns = ["symbol_name", "price_usd", "volume_traded"]
        cleaned = exporter._clean_column_names(columns)
        assert len(cleaned) == 3

    def test_health_check(self, exporter):
        health = exporter.health_check()
        assert health is not None
        assert health["status"] == "healthy"
        assert health["export_dir_exists"] is True

    def test_export_to_excel_simple(self, exporter):
        data = {
            "symbol": ["AAPL", "MSFT"],
            "price": [150.0, 370.0]
        }
        result = exporter.export_to_excel(data, "test_export")
        assert result is not None
        assert os.path.exists(result)
        assert result.endswith(".xlsx")

    def test_export_to_excel_none(self, exporter):
        result = exporter.export_to_excel(None, "test_export")
        assert result is None

    def test_export_to_excel_list(self, exporter):
        data = [
            {"symbol": "AAPL", "price": 150.0},
            {"symbol": "MSFT", "price": 370.0}
        ]
        result = exporter.export_to_excel(data, "test_list")
        assert result is not None
        assert os.path.exists(result)

    def test_export_multi_sheet_valid(self, exporter):
        data_dict = {
            "Sheet1": pd.DataFrame({
                "symbol": ["AAPL", "MSFT"],
                "price": [150.0, 370.0]
            }),
            "Sheet2": pd.DataFrame({
                "symbol": ["BTC", "ETH"],
                "price": [45000.0, 2500.0]
            })
        }
        result = exporter.export_multi_sheet(data_dict, "test_multi")
        assert result is not None
        assert os.path.exists(result)

    def test_export_multi_sheet_empty(self, exporter):
        result = exporter.export_multi_sheet({}, "test_multi")
        assert result is None

    def test_ensure_export_directory(self, temp_export_dir):
        exporter = ExcelExporter(export_dir=os.path.join(temp_export_dir, "nested", "dir"))
        assert exporter.export_dir.exists()


class TestMLExcelHelper:

    def test_prepare_prediction_sheet_dataframe(self):
        df = pd.DataFrame({
            "symbol": ["AAPL", "MSFT"],
            "prediction": [0.85, 0.92]
        })
        result = MLExcelHelper.prepare_prediction_sheet(df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_prepare_prediction_sheet_dict(self):
        data = {
            "AAPL": 0.85,
            "MSFT": 0.92
        }
        result = MLExcelHelper.prepare_prediction_sheet(data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_prepare_feature_sheet(self):
        importance = {
            "rsi": 0.25,
            "ema": 0.30,
            "volume": 0.20
        }
        result = MLExcelHelper.prepare_feature_sheet(importance)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

    def test_prepare_metadata_sheet(self):
        info = {
            "model": "RandomForest",
            "accuracy": 0.92
        }
        result = MLExcelHelper.prepare_metadata_sheet(info)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_format_pair_name_valid(self):
        result = MLExcelHelper._format_pair_name("btc_usdt")
        assert isinstance(result, str)

    def test_format_pair_name_none(self):
        result = MLExcelHelper._format_pair_name(None)
        assert result in ["", "None"]  # Implementation may return either

    def test_format_dataframe_columns(self):
        df = pd.DataFrame({
            "symbol_name": [1, 2],
            "price_usd": [100.0, 200.0]
        })
        result = MLExcelHelper._format_dataframe_columns(df)
        assert isinstance(result, pd.DataFrame)

    def test_prepare_ml_ready_data_valid(self):
        data = {
            "symbols": {
                "BTC": {"price": 45000.0},
                "AAPL": {"price": 150.0}
            }
        }
        result = MLExcelHelper.prepare_ml_ready_data(data)
        assert isinstance(result, dict)

    def test_prepare_ml_ready_data_empty(self):
        result = MLExcelHelper.prepare_ml_ready_data({})
        assert isinstance(result, dict)

    def test_process_sheet_timestamps(self):
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "timestamp": ["2025-12-10T10:00:00Z"],
            "price": [150.0]
        })
        result = MLExcelHelper._process_sheet_timestamps(df)
        assert isinstance(result, pd.DataFrame)
