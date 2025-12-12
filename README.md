# Market Data Automation System

## 1. Executive Summary

The **Market Data Automation System** is a specialized automation engine built to streamline the collection of financial market data. It connects directly to stock markets (via Yahoo Finance) and cryptocurrency exchanges (via Binance) to fetch real-time pricing and volume data without human intervention.

**The Problem:** Financial analysts and data scientists often waste valuable time manually downloading CSVs, cleaning messy data, and merging files from different sources. This process is slow, error-prone, and unscalable.

**The Solution:** This system automates the entire pipeline. It fetches raw data, standardizes it into a clean, unified format, and instantly generates:
*   **Excel Reports:** For immediate human analysis and presentation.
*   **CSV/JSON Datasets:** For machine learning training and algorithmic trading backtesting.

The result is a reliable, "always-on" data factory that delivers fresh, consistent market intelligence.

---

## 2. System Architecture & Capabilities

The system is built upon a modular, event-driven architecture using Python. It is designed for high availability, fault tolerance, and strict data integrity.

### 2.1 Core Components

*   **Data Ingestion Layer (LiveDataFetcher):** A robust fetching engine that interfaces with external APIs.
    *   **Yahoo Finance (yfinance):** Selected for Equities due to its comprehensive coverage of global stock markets and reliable historical data.
    *   **Binance API:** Selected for Cryptocurrencies due to its status as the highest-volume exchange, providing the most accurate real-time price discovery.
    It implements intelligent rate limiting, automatic retries with exponential backoff, and connection pooling to ensure stability under network stress.

*   **Aggregation Engine (DataAggregator):** A processing core that normalizes heterogeneous data structures into a unified schema. It handles data type validation, missing value imputation, and the computation of derived metrics (e.g., spread, volatility).

*   **Export Orchestration (PipelineOrchestrator):** The central controller that manages the data flow. It utilizes a `RunIdManager` to ensure synchronization across multiple export formats, guaranteeing that an Excel report and its corresponding CSV dump represent the exact same temporal snapshot.

*   **Persistence Layer (Exporters):**
    *   *ExcelExporter (Human-Centric):* Generates formatted .xlsx files designed for **human analysis** and boardroom presentations. Features split date/time columns and conditional formatting for easy manual review.
    *   *CsvExporter & JSON (Machine-Centric):* Produces structured .csv and .json files optimized for **Machine Learning datasets**, database ingestion, and algorithmic processing.

### 2.2 Data Flow & Module Interaction

The modules function as a cohesive, synchronized pipeline:

1.  **LiveDataFetcher** pulls raw market data from external APIs (Yahoo/Binance).
2.  **DataAggregator** normalizes this raw data into a unified, consistent schema.
3.  **PipelineOrchestrator** receives the processed data and generates a unique **Run ID**.
4.  **Exporters** receive the data and the synchronized Run ID to generate matching output files in `data/exports/`.

### 2.3 Key Features

*   **Multi-Asset Support:** Native handling of both Equity (Stocks) and Digital Assets (Crypto) within a single pipeline.
*   **Synchronized Export:** Atomic generation of multi-format outputs (JSON, Excel, CSV) sharing a unique, incremental Run ID.
*   **Self-Healing Configuration:** Automatically detects if `config/settings.json` is missing and creates it from `settings_example.json`, ensuring immediate usability.
*   **Automated Environment Setup:** Upon execution, the system automatically provisions the necessary directory structure, creating the `logs/` folder for system logs and `data/exports/` for output files without user intervention.
*   **Logging:** Comprehensive logging (JSON/Text) with rotation policies, error segregation, and performance metrics tracking.
*   **ML-Ready Infrastructure:** While Machine Learning models are not currently integrated, the system's architecture (data normalization, feature engineering, and consistent schema) is fully prepared to support future ML model training and inference.

---

## 3. Technical Implementation & Contributions

I have engineered this system with a focus on **reliability** and **maintainability**. Below is a detailed account of the technical decisions and implementations I have executed:

### 3.1 Concurrency & Synchronization

I identified a critical race condition where separate export processes could drift out of sync, leading to mismatched file identifiers. To resolve this, I implemented a thread-safe Singleton pattern in the `RunIdManager` class. This ensures that for every execution cycle, a single, immutable Run ID is generated and propagated to all exporters, guaranteeing referential integrity across file formats.

### 3.2 Robust Error Handling

I architected the system to be resilient against external failures. The fetching logic includes specific handlers for API timeouts and malformed responses. In the export layer, I implemented file locking mechanisms to prevent write conflicts and added fallback logic to handle permission errors gracefully.

### 3.3 Configuration Management

I designed a fail-safe configuration loader. Recognizing that deployment environments vary, I wrote logic to automatically bootstrap the `settings.json` file from a version-controlled example if the primary configuration is absent. This ensures the application is "clone-and-run" ready without manual setup intervention.

---

## 4. Installation & Configuration

### 4.1 Prerequisites

*   Python 3.8 or higher
*   pip package manager

### 4.2 Installation Steps

```bash
# 1. Clone the repository
git clone <repository_url>
cd market-data-automation-system

# 2. Create a virtual environment (Recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

### 4.3 Configuration Architecture

The system relies on a critical configuration file: `config/settings.json`.

*   **settings_example.json (Template):** A version-controlled template containing safe default values. It serves as the "factory reset" state.
*   **settings.json (Active Config):** The live configuration file used by the application. It is **critical** for operation. If missing, the system auto-generates it from the template.

**Key Configuration Toggles:**

*   `scheduler.enabled`: Controls the automated loop.
    *   **true:** Runs indefinitely every 30 seconds (default interval).
    *   **false:** (Default) Disables the loop for controlled, single-execution runs (e.g., via Cron or manual testing).
*   `data_sources.symbols`: List of ticker symbols (e.g., "AAPL", "BTCUSDT").
*   `csv_export.enabled`: Set to `true` to enable CSV generation.

---

## 5. Usage Guide

### 5.1 Execution Methods

The system is designed to be flexible and can be executed in multiple ways depending on the use case:

#### Method 1: Single Run (Manual / Cron)

Ideal for testing or scheduled tasks via external schedulers (like Linux Cron). This ignores the internal scheduler setting and runs the pipeline exactly once.

```bash
python src/main.py --run-once
```

#### Method 2: Internal Scheduler (Daemon Mode)

To run the system continuously (e.g., every 30 seconds), set `"scheduler": { "enabled": true }` in `settings.json` and run:

```bash
python src/main.py
```

### 5.2 Command Line Arguments

| Argument | Description |
| :--- | :--- |
| `--run-once` | Executes the pipeline a single time and exits. |
| `--verbose` | Enables DEBUG level logging for detailed diagnostics. |
| `--config <path>` | Specifies a custom configuration file path. |

---

## 6. Extensibility & Future Capabilities

The system's modular architecture is designed to support seamless integration of advanced capabilities. While not currently implemented, the codebase is structured to easily accommodate:

*   **Database Integration:** The exporter interface can be extended to support PostgreSQL/TimescaleDB for long-term time-series storage.
*   **Real-time Dashboard:** The event-driven design allows for a WebSocket server attachment to stream live market updates to a frontend UI.
*   **Advanced Analytics:** The data pipeline is pre-configured to pipe normalized data into `scikit-learn` for predictive trend analysis and anomaly detection.
*   **Plug-and-Play Machine Learning:** The modular design allows for seamless injection of custom ML models. Developers can simply drop in a model file and register it, enabling immediate inference on the live data stream without rewriting core logic.
*   **Containerization:** The stateless nature of the fetcher allows for straightforward Docker containerization and Kubernetes deployment.

---

*System Created and Documentation Written by Aritro Biswas. Last Updated: December 2025.*
