
# ⚡ UK National Grid | GreenOps Telemetry Lakehouse

### Enterprise Medallion Lakehouse Architecture with dbt Core, DuckDB, Delta Lake & Real-Time Geospatial Workload Orchestration

[![CI Pipeline](https://github.com/Sonexis7/uk-carbon-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Sonexis7/uk-carbon-lakehouse/actions)
![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/Engine-DuckDB-FFF000?logo=duckdb&logoColor=black)
![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8?logo=apache-spark&logoColor=white)
![dbt Core](https://img.shields.io/badge/Governance-dbt--duckdb%201.8-FF694B?logo=dbt&logoColor=white)
![Streamlit](https://img.shields.io/badge/UI-Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Plotly](https://img.shields.io/badge/Visualization-Plotly-3F4F75?logo=plotly&logoColor=white)

---

![UK National Grid GreenOps Lakehouse Banner](assets/PROJECT%20THUMBNAIL.png)

---

## Executive Summary & Problem Statement

Modern data centers, high-performance computing clusters, and AI model training workloads consume gigawatt-hours of electricity every day. Executing computational pipelines indiscriminately across grid networks incurs two major liabilities:

* **Severe Scope 2 Carbon Footprints:** Compute executed during peak fossil-fuel windows (coal and open-cycle gas turbine peakers) generates up to 330+ gCO₂/kWh.
* **Excessive Cloud & Power Expenditure:** Peak grid intervals coincide with commercial electricity tariffs that are 3x to 4x higher than off-peak renewable curtailment hours.

This platform implements an enterprise GreenOps Lakehouse tracking all 14 UK Distribution Network Operator (DNO) regions in real time. By decoupling telemetry ingestion into an ACID Delta Lake storage layer and modeling workload eligibility through governed dbt marts, the system enables automated spatial and temporal compute arbitrage—routing computational batch jobs to green regions (e.g., North Scotland at 0 gCO₂/kWh) and deferring workloads during high-carbon grid congestion.

---

## 🏛️ Lakehouse Architecture & Data Flow

The platform is designed around the industry-standard Medallion Architecture (Bronze ➔ Silver ➔ Gold), decoupling compute (DuckDB) from storage (Delta Lake) and enforcing strict declarative schema contracts:

```text
 ┌────────────────────────────────────────────────────────┐
 │ UK National Grid ESO Live Carbon Telemetry API        │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼ (HTTP REST / Polling Ingest)
 ┌────────────────────────────────────────────────────────┐
 │ BRONZE LAYER (Raw Telemetry Landing Store)             │
 │ • Immutable landing storage of regional snapshots      │
 │ • Format: Raw JSON / Staging Parquet                   │
 │ • Lineage: Ingestion metadata injection (_batch_id)    │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼ (Normalize, Validate, Delta Write)
 ┌────────────────────────────────────────────────────────┐
 │ SILVER LAYER (ACID Delta Lake Storage)                 │
 │ • Storage: Delta Lake format (fact_regional_carbon)    │
 │ • Snappy-compressed Parquet with ACID transaction log │
 │ • Schema gate: sum(generation_mix_perc) <= 105%        │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼ (dbt Core + dbt-duckdb Adapter)
 ┌────────────────────────────────────────────────────────┐
 │ GOLD LAYER (dbt-Governed Analytics Mart)               │
 │ • Target: analytics.duckdb -> mart_green_compute_...   │
 │ • Modeling: Ranked dispatch tiers (OPTIMAL, DEFER)     │
 │ • Contracts: Declarative unique, not_null assertions   │
 └─────────────┬───────────────────────────┬──────────────┘
               │                           │
               ▼                           ▼
 ┌───────────────────────────┐ ┌──────────────────────────┐
 │ Financial Audit & CI Gate │ │ Streamlit & Plotly UI    │
 │ • GitHub Actions CI Run   │ │ • UK Vector Tile Map     │
 │ • Automated Regression    │ │ • GreenOps ROI Modeler   │
 └───────────────────────────┘ └──────────────────────────┘

```

---

## 📸 Executive Visual Showcase

| 1. Geospatial Telemetry Command Center | 2. Compute Dispatch Routing Matrix |
| --- | --- |
|  |  |
| *Real-time Plotly vector map tracking regional grid intensity across all 14 UK DNO zones.* | *Sorted dispatch matrix assigning batch workloads to zero-carbon windows based on live grid generation.* |

| 3. dbt Data Governance & Delta Lineage | 4. FinOps Workload Arbitrage Simulator |
| --- | --- |
|  |  |
| *Zero-copy Delta Lake inspection querying generation mix percentages directly through DuckDB.* | *B2B ROI model quantifying direct cost and carbon reductions by shifting batch workloads.* |

---

## 💰 The GreenOps Business Case: Cloud Power Arbitrage

Modern hyperscale data centers and GPU compute clusters run massive overnight batch workloads (ETL pipelines, machine learning training, financial simulations). The integrated simulator calculates the financial and carbon arbitrage achieved by shifting compute windows:

$$\text{Annual Net Savings} = (\text{kW Draw} \times \text{Hours/Day} \times 365) \times (\text{Tariff}_{\text{Peak}} - \text{Tariff}_{\text{Green}})$$

$$\text{Percentage Reduction} = \left( \frac{\text{Tariff}_{\text{Peak}} - \text{Tariff}_{\text{Green}}}{\text{Tariff}_{\text{Peak}}} \right) \times 100$$

### Production Benchmark Scenario

* **Cluster Baseline:** 75 kW average compute cluster power draw
* **Daily Batch Window:** 6 hours/day
* **Annual Power Volume:** 164,250 kWh
* **Peak Grid Tariff:** £0.29/kWh (South Wales @ 336 gCO₂/kWh)
* **Off-Peak Renewable Tariff:** £0.09/kWh (North Scotland @ 0 gCO₂/kWh)
* **Financial Arbitrage Generated:** £32,850.00/year (68.9% reduction in electricity spend)

---

## 🛡️ Declarative Data Governance & Contracts

Data quality is enforced through declarative schema contracts inside `dbt_lakehouse/models/gold/schema.yml` running against DuckDB:

```yaml
version: 2

models:
  - name: mart_green_compute_dispatch
    description: "Enterprise Gold Mart governing green-window compute scheduling."
    columns:
      - name: region
        description: "UK DNO Region Name"
        tests:
          - unique
          - not_null

      - name: forecast_gco2_kwh
        description: "Carbon intensity in gCO2/kWh"
        tests:
          - not_null

      - name: dispatch_tier
        description: "Operational routing decision tier"
        tests:
          - accepted_values:
              values: ['OPTIMAL', 'STANDARD', 'DEFER']

```

### Automated Quality Validation Status

Running `dbt test` validates all constraints directly against DuckDB:

* `PASS 14 unique_mart_green_compute_dispatch_region`
* `PASS 14 not_null_mart_green_compute_dispatch_region`
* `PASS 14 not_null_mart_green_compute_dispatch_forecast_gco2_kwh`
* `PASS 14 accepted_values_mart_green_compute_dispatch_dispatch_tier`

---

## ⚙️ Tech Stack & Production Tooling

* **Analytical Storage:** Delta Lake (`deltalake==0.15.3`) providing ACID transactional logs, schema enforcement, and time-travel capability.
* **Query Engine:** DuckDB providing vectorized, in-process columnar SQL execution over Parquet and Delta tables.
* **Transformation & Testing:** dbt Core (`dbt-duckdb`) for declarative modeling, data contracts, and lineage DAG compilation.
* **Presentation Layer:** Streamlit with custom glassmorphism styling and Plotly vector maps.
* **CI/CD Automation:** GitHub Actions validating data ingestion, dbt models, and schema contracts on every push.

---

## 📁 Repository Structure

```text
uk-carbon-lakehouse/
├── .github/
│   └── workflows/
│       └── ci.yml               # Automated GitHub Actions CI pipeline
├── assets/                      # Production UI screenshots and architecture visuals
│   ├── PROJECT THUMBNAIL.png    # High-resolution platform overview card
│   ├── pt1.png                  # Geospatial telemetry map view
│   ├── pt2.png                  # Compute dispatch routing matrix
│   ├── pt3.png                  # dbt data governance and Delta logs
│   └── pt4.png                  # FinOps compute arbitrage simulator
├── dbt_lakehouse/               # dbt Core analytics project
│   ├── models/
│   │   └── gold/
│   │       ├── mart_green_compute_dispatch.sql
│   │       └── schema.yml       # Data contracts and schema tests
│   ├── dbt_project.yml
│   └── profiles.yml             # DuckDB adapter connection profile
├── app.py                       # High-end Streamlit & Plotly dark-mode dashboard
├── audit_engine.py              # Financial ROI modeler & pipeline validation suite
├── pipeline.py                  # Medallion ingestion engine (Bronze -> Silver Delta)
├── requirements.txt             # Pinned enterprise dependencies
└── README.md

```

---

## 🚀 Local Development & Quickstart

### Prerequisites

* Python 3.11+
* Git

### 1. Clone Repository & Setup Virtual Environment

```bash
git clone [https://github.com/Sonexis7/uk-carbon-lakehouse.git](https://github.com/Sonexis7/uk-carbon-lakehouse.git)
cd uk-carbon-lakehouse

# Create and activate Python 3.11 virtual environment
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1    # On Windows
source venv/bin/activate       # On Linux/macOS

# Install pinned production dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

```

### 2. Run the Ingestion Pipeline

Ingests live UK National Grid telemetry, generates Bronze landing records, and writes partitioned Silver Delta Lake logs:

```bash
python pipeline.py
python audit_engine.py

```

### 3. Compile dbt Models & Run Data Contracts

```bash
cd dbt_lakehouse
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..

```

### 4. Launch the Interactive Dashboard

```bash
streamlit run app.py

```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## 🔄 Automated CI/CD Pipeline

Every commit and pull request to `main` triggers `.github/workflows/ci.yml`, running on a headless `ubuntu-latest` container:

1. Provisions a clean Python 3.11 runtime.
2. Ingests live UK National Grid telemetry into Delta Lake.
3. Executes the dbt transformation pipeline and builds `analytics.duckdb`.
4. Runs all declarative schema assertions (`unique`, `not_null`, `accepted_values`).

---

## 📜 License & Compliance

Distributed under the Apache 2.0 License. Telemetry data derived from the UK National Grid ESO Open Data Portal under the Open Government Licence (OGL).
'@ -Encoding utf8

```

---

### Push the New File to GitHub

Once executed, stage, commit, and push in one go:

```powershell
git add README.md
git commit -m "docs: publish pure markdown README with zero HTML tags"
git push origin main

```