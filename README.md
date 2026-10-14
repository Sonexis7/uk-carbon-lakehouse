# UK National Grid Clean Compute Lakehouse

An end-to-end data platform simulating an enterprise **Microsoft Fabric Lakehouse** using open-source modern data tools (**Delta-RS, DuckDB, Pandas, and Streamlit**) with zero cloud infrastructure overhead.

## Architecture

[ NESO Carbon Intensity API ]
│
▼ (HTTP / JSON)
[ Bronze Layer ] Immutable Raw Telemetry Storage
│
▼ (Pandas + Schema Contract Validation Gate)
[ Silver Layer ] Governed ACID Delta Lake Table (Partitioned by Date)
│
▼ (DuckDB In-Memory Columnar OLAP Engine)
[ Gold Layer ]   Green Compute Decision Mart (Parquet)
│
▼
[ Presentation ] Streamlit Interactive Operational Dashboard


## Microsoft Fabric 1:1 Concept Mapping

| Local Stack Component | Microsoft Fabric Equivalent | Architectural Responsibility |
| :--- | :--- | :--- |
| `requests` Pipeline Logic | **Data Factory Pipeline** | Scheduled extraction from external REST API |
| `./lakehouse/` Directory | **OneLake** | Unified single-store landing abstraction |
| `deltalake` (Delta-RS) | **Fabric Delta Tables** | ACID transaction logs (`_delta_log`), schema enforcement |
| Pre-Commit Quality Gate | **Data Quality / Fabric Notebooks** | Data contract validation and anomaly prevention |
| `duckdb` SQL Engine | **SQL Analytics Endpoint** | High-performance columnar SQL across Delta Parquet |
| Streamlit Application | **Power BI Direct Lake** | Low-latency operational metric consumption |

## Data Governance & Audit Controls
- **Audit Lineage:** Every record in the Silver Delta table contains system metadata: `_ingested_at`, `_source_system`, and a unique `_batch_id`.
- **Pre-Commit Quality Gate:** Pre-commit assertions verify non-negative carbon intensities, composite key validity, and fuel percentage integrity (<105%) before transaction logs commit.
- **Partition Pruning:** Silver Delta layer is partitioned by `date`, optimizing scan speeds for downstream queries.

## Quickstart

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the Medallion Pipeline
python pipeline.py

# 3. Launch the Analytical Dashboard
streamlit run app.py

---

### Execution Pathway: Step-by-Step Commands

Run these terminal commands inside your project directory to execute the pipeline and view the dashboard:

```bash
# Step 1: Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate       # On macOS/Linux
# .venv\Scripts\activate        # On Windows

# Step 2: Install dependencies
pip install -r requirements.txt

# Step 3: Run the end-to-end pipeline
python pipeline.py

# Step 4: Launch the Streamlit dashboard
streamlit run app.py