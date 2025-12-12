<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Market Data Automation System - Technical Documentation</title>
</head>
<body>

    <h1 style="border-bottom: 2px solid #333; padding-bottom: 10px;">Market Data Automation System</h1>

    <h2>1. Executive Summary</h2>
    <p>
        The <strong>Market Data Automation System</strong> is a specialized automation engine built to streamline the collection of financial market data. It connects directly to stock markets (via Yahoo Finance) and cryptocurrency exchanges (via Binance) to fetch real-time pricing and volume data without human intervention.
    </p>
    <p>
        <strong>The Problem:</strong> Financial analysts and data scientists often waste valuable time manually downloading CSVs, cleaning messy data, and merging files from different sources. This process is slow, error-prone, and unscalable.
    </p>
    <p>
        <strong>The Solution:</strong> This system automates the entire pipeline. It fetches raw data, standardizes it into a clean, unified format, and instantly generates:
        <ul>
            <li><strong>Excel Reports:</strong> For immediate human analysis and presentation.</li>
            <li><strong>CSV/JSON Datasets:</strong> For machine learning training and algorithmic trading backtesting.</li>
        </ul>
        The result is a reliable, "always-on" data factory that delivers fresh, consistent market intelligence.
    </p>

    <hr>

    <h2>2. System Architecture & Capabilities</h2>
    <p>
        The system is built upon a modular, event-driven architecture using Python. It is designed for high availability, fault tolerance, and strict data integrity.
    </p>

    <h3>2.1 Core Components</h3>
    <ul>
        <li><strong>Data Ingestion Layer (LiveDataFetcher):</strong> A robust fetching engine that interfaces with external APIs.
            <ul>
                <li><strong>Yahoo Finance (yfinance):</strong> Selected for Equities due to its comprehensive coverage of global stock markets and reliable historical data.</li>
                <li><strong>Binance API:</strong> Selected for Cryptocurrencies due to its status as the highest-volume exchange, providing the most accurate real-time price discovery.</li>
            </ul>
            It implements intelligent rate limiting, automatic retries with exponential backoff, and connection pooling to ensure stability under network stress.
        </li>
        <li><strong>Aggregation Engine (DataAggregator):</strong> A processing core that normalizes heterogeneous data structures into a unified schema. It handles data type validation, missing value imputation, and the computation of derived metrics (e.g., spread, volatility).</li>
        <li><strong>Export Orchestration (PipelineOrchestrator):</strong> The central controller that manages the data flow. It utilizes a <code>RunIdManager</code> to ensure synchronization across multiple export formats, guaranteeing that an Excel report and its corresponding CSV dump represent the exact same temporal snapshot.</li>
        <li><strong>Persistence Layer (Exporters):</strong>
            <ul>
                <li><em>ExcelExporter (Human-Centric):</em> Generates formatted .xlsx files designed for <strong>human analysis</strong> and boardroom presentations. Features split date/time columns and conditional formatting for easy manual review.</li>
                <li><em>CsvExporter & JSON (Machine-Centric):</em> Produces structured .csv and .json files optimized for <strong>Machine Learning datasets</strong>, database ingestion, and algorithmic processing.</li>
            </ul>
        </li>
    </ul>

    <h3>2.2 Data Flow & Module Interaction</h3>
    <p>
        The modules function as a cohesive, synchronized pipeline:
    </p>
    <ol>
        <li><strong>LiveDataFetcher</strong> pulls raw market data from external APIs (Yahoo/Binance).</li>
        <li><strong>DataAggregator</strong> normalizes this raw data into a unified, consistent schema.</li>
        <li><strong>PipelineOrchestrator</strong> receives the processed data and generates a unique <strong>Run ID</strong>.</li>
        <li><strong>Exporters</strong> receive the data and the synchronized Run ID to generate matching output files in <code>data/exports/</code>.</li>
    </ol>

    <h3>2.3 Key Features</h3>
    <ul>
        <li><strong>Multi-Asset Support:</strong> Native handling of both Equity (Stocks) and Digital Assets (Crypto) within a single pipeline.</li>
        <li><strong>Synchronized Export:</strong> Atomic generation of multi-format outputs (JSON, Excel, CSV) sharing a unique, incremental Run ID.</li>
        <li><strong>Self-Healing Configuration:</strong> Automatically detects if <code>config/settings.json</code> is missing and creates it from <code>settings_example.json</code>, ensuring immediate usability.</li>
        <li><strong>Automated Environment Setup:</strong> Upon execution, the system automatically provisions the necessary directory structure, creating the <code>logs/</code> folder for system logs and <code>data/exports/</code> for output files without user intervention.</li>
        <li><strong>Logging:</strong> Comprehensive logging (JSON/Text) with rotation policies, error segregation, and performance metrics tracking.</li>
        <li><strong>ML-Ready Infrastructure:</strong> While Machine Learning models are not currently integrated, the system's architecture (data normalization, feature engineering, and consistent schema) is fully prepared to support future ML model training and inference.</li>
    </ul>

    <hr>

    <h2>3. Technical Implementation & Contributions</h2>
    <p>
        I have engineered this system with a focus on <strong>reliability</strong> and <strong>maintainability</strong>. Below is a detailed account of the technical decisions and implementations I have executed:
    </p>

    <h3>3.1 Concurrency & Synchronization</h3>
    <p>
        I identified a critical race condition where separate export processes could drift out of sync, leading to mismatched file identifiers. To resolve this, I implemented a thread-safe Singleton pattern in the <code>RunIdManager</code> class. This ensures that for every execution cycle, a single, immutable Run ID is generated and propagated to all exporters, guaranteeing referential integrity across file formats.
    </p>

    <h3>3.2 Robust Error Handling</h3>
    <p>
        I architected the system to be resilient against external failures. The fetching logic includes specific handlers for API timeouts and malformed responses. In the export layer, I implemented file locking mechanisms to prevent write conflicts and added fallback logic to handle permission errors gracefully.
    </p>

    <h3>3.3 Configuration Management</h3>
    <p>
        I designed a fail-safe configuration loader. Recognizing that deployment environments vary, I wrote logic to automatically bootstrap the <code>settings.json</code> file from a version-controlled example if the primary configuration is absent. This ensures the application is "clone-and-run" ready without manual setup intervention.
    </p>

    <hr>

    <h2>4. Installation & Configuration</h2>

    <h3>4.1 Prerequisites</h3>
    <ul>
        <li>Python 3.8 or higher</li>
        <li>pip package manager</li>
    </ul>

    <h3>4.2 Installation Steps</h3>
    <pre style="background-color: #f4f4f4; padding: 10px; border: 1px solid #ddd;">
# 1. Clone the repository
git clone &lt;repository_url&gt;
cd market-data-automation-system

# 2. Create a virtual environment (Recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
    </pre>

    <h3>4.3 Configuration Architecture</h3>
    <p>
        The system relies on a critical configuration file: <code>config/settings.json</code>.
    </p>
    <ul>
        <li><strong>settings_example.json (Template):</strong> A version-controlled template containing safe default values. It serves as the "factory reset" state.</li>
        <li><strong>settings.json (Active Config):</strong> The live configuration file used by the application. It is <strong>critical</strong> for operation. If missing, the system auto-generates it from the template.</li>
    </ul>
    <p><strong>Key Configuration Toggles:</strong></p>
    <ul>
        <li><code>scheduler.enabled</code>: Controls the automated loop.
            <ul>
                <li><strong>true:</strong> Runs indefinitely every 30 seconds (default interval).</li>
                <li><strong>false:</strong> (Default) Disables the loop for controlled, single-execution runs (e.g., via Cron or manual testing).</li>
            </ul>
        </li>
        <li><code>data_sources.symbols</code>: List of ticker symbols (e.g., "AAPL", "BTCUSDT").</li>
        <li><code>csv_export.enabled</code>: Set to <code>true</code> to enable CSV generation.</li>
    </ul>

    <hr>

    <h2>5. Usage Guide</h2>

    <h3>5.1 Execution Methods</h3>
    <p>
        The system is designed to be flexible and can be executed in multiple ways depending on the use case:
    </p>
    <h4>Method 1: Single Run (Manual / Cron)</h4>
    <p>
        Ideal for testing or scheduled tasks via external schedulers (like Linux Cron). This ignores the internal scheduler setting and runs the pipeline exactly once.
    </p>
    <pre style="background-color: #f4f4f4; padding: 10px; border: 1px solid #ddd;">
python src/main.py --run-once
    </pre>

    <h4>Method 2: Internal Scheduler (Daemon Mode)</h4>
    <p>
        To run the system continuously (e.g., every 30 seconds), set <code>"scheduler": { "enabled": true }</code> in <code>settings.json</code> and run:
    </p>
    <pre style="background-color: #f4f4f4; padding: 10px; border: 1px solid #ddd;">
python src/main.py
    </pre>

    <h3>5.2 Command Line Arguments</h3>
    <table border="1" cellpadding="10" cellspacing="0" style="border-collapse: collapse; width: 100%;">
        <thead>
            <tr style="background-color: #eee;">
                <th>Argument</th>
                <th>Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><code>--run-once</code></td>
                <td>Executes the pipeline a single time and exits.</td>
            </tr>
            <tr>
                <td><code>--verbose</code></td>
                <td>Enables DEBUG level logging for detailed diagnostics.</td>
            </tr>
            <tr>
                <td><code>--config &lt;path&gt;</code></td>
                <td>Specifies a custom configuration file path.</td>
            </tr>
        </tbody>
    </table>

    <hr>

    <h2>6. Extensibility & Future Capabilities</h2>
    <p>
        The system's modular architecture is designed to support seamless integration of advanced capabilities. While not currently implemented, the codebase is structured to easily accommodate:
    </p>
    <ul>
        <li><strong>Database Integration:</strong> The exporter interface can be extended to support PostgreSQL/TimescaleDB for long-term time-series storage.</li>
        <li><strong>Real-time Dashboard:</strong> The event-driven design allows for a WebSocket server attachment to stream live market updates to a frontend UI.</li>
        <li><strong>Advanced Analytics:</strong> The data pipeline is pre-configured to pipe normalized data into <code>scikit-learn</code> for predictive trend analysis and anomaly detection.</li>
        <li><strong>Plug-and-Play Machine Learning:</strong> The modular design allows for seamless injection of custom ML models. Developers can simply drop in a model file and register it, enabling immediate inference on the live data stream without rewriting core logic.</li>
        <li><strong>Containerization:</strong> The stateless nature of the fetcher allows for straightforward Docker containerization and Kubernetes deployment.</li>
    </ul>

    <hr>
    <p style="font-size: 0.9em; color: #666;">
        <em>System Created and Documentation Written by Aritro Biswas. Last Updated: December 2025.</em>
    </p>

</body>
</html>
