import os
import json
import uuid
import requests
import pandas as pd
from datetime import datetime
import duckdb
from deltalake import write_deltalake

# -------------------------------------------------------------
# 1. STORAGE PATHS (Simulating OneLake Landing Zones)
# -------------------------------------------------------------
BASE_DIR = "./lakehouse"
BRONZE_DIR = os.path.join(BASE_DIR, "bronze", "neso_carbon")
SILVER_DIR = os.path.join(BASE_DIR, "silver", "fact_regional_carbon")
GOLD_DIR = os.path.join(BASE_DIR, "gold")

for folder in [BRONZE_DIR, GOLD_DIR]:
    os.makedirs(folder, exist_ok=True)

# -------------------------------------------------------------
# 2. DATA GOVERNANCE CONTRACT (Pre-Commit Quality Gate)
# -------------------------------------------------------------
def validate_data_contract(df: pd.DataFrame) -> bool:
    """
    Enforces quality assertions before committing to Silver.
    Prevents corrupt or negative telemetry from entering Delta Lake.
    """
    if (df["forecast_gco2_kwh"] < 0).any():
        raise ValueError("[Governance Gate Failed] Negative carbon intensity values found.")
    
    total_fuel_check = (
        df["gas_perc"] + df["coal_perc"] + df["wind_perc"] + 
        df["solar_perc"] + df["nuclear_perc"] + df["biomass_perc"] + 
        df["imports_perc"]
    )
    if (total_fuel_check > 105).any():
        raise ValueError("[Governance Gate Failed] Generation mix sum exceeds valid threshold (>100%).")
    
    if df["region_id"].isnull().any() or df["from_time"].isnull().any():
        raise ValueError("[Governance Gate Failed] Null values detected in mandatory composite keys.")
        
    print("🛡️  [Governance] Quality Contract Verified: Schema, keys, and values valid.")
    return True

# -------------------------------------------------------------
# 3. BRONZE LAYER (Raw Extraction & Immutable Archive)
# -------------------------------------------------------------
def ingest_bronze() -> dict:
    print("⏳ [Bronze] Requesting telemetry from NESO / UK National Grid API...")
    url = "https://api.carbonintensity.org.uk/regional"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    payload = response.json()

    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    raw_file = os.path.join(BRONZE_DIR, f"snapshot_{timestamp_str}.json")

    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    print(f"✅ [Bronze] Raw JSON snapshot archived to: {raw_file}")
    return payload

# -------------------------------------------------------------
# 4. SILVER LAYER (Unnesting, Lineage & ACID Delta Lake Write)
# -------------------------------------------------------------
def transform_silver(payload: dict) -> None:
    print("⏳ [Silver] Parsing nested structures and enforcing schema...")
    data_entry = payload["data"][0]
    from_time = pd.to_datetime(data_entry["from"])
    to_time = pd.to_datetime(data_entry["to"])
    batch_id = str(uuid.uuid4())[:8]

    records = []
    for r in data_entry["regions"]:
        fuel_dict = {f["fuel"]: float(f["perc"]) for f in r.get("generationmix", [])}
        
        forecast_val = r.get("intensity", {}).get("forecast")
        index_val = r.get("intensity", {}).get("index")

        records.append({
            # Business Keys
            "region_id": int(r["regionid"]),
            "region_name": str(r["dnoregion"]),
            "short_name": str(r["shortname"]),
            "from_time": from_time,
            "to_time": to_time,
            "date": from_time.strftime("%Y-%m-%d"),
            # Telemetry Metrics
            "forecast_gco2_kwh": float(forecast_val) if forecast_val is not None else 0.0,
            "carbon_index": str(index_val) if index_val is not None else "unknown",
            "gas_perc": fuel_dict.get("gas", 0.0),
            "wind_perc": fuel_dict.get("wind", 0.0),
            "solar_perc": fuel_dict.get("solar", 0.0),
            "nuclear_perc": fuel_dict.get("nuclear", 0.0),
            "biomass_perc": fuel_dict.get("biomass", 0.0),
            "coal_perc": fuel_dict.get("coal", 0.0),
            "imports_perc": fuel_dict.get("imports", 0.0),
            # Data Lineage & Governance Columns
            "_ingested_at": datetime.utcnow(),
            "_source_system": "NESO_National_Grid_API",
            "_batch_id": batch_id
        })

    silver_df = pd.DataFrame(records)
    silver_df["renewable_share_perc"] = silver_df["wind_perc"] + silver_df["solar_perc"]
    silver_df["fossil_share_perc"] = silver_df["gas_perc"] + silver_df["coal_perc"]

    # Execute Pre-Commit Governance Gate
    validate_data_contract(silver_df)

    # Write ACID Delta Table partitioned by Date
    write_deltalake(
        SILVER_DIR,
        silver_df,
        mode="append",
        partition_by=["date"]
    )
    print(f"✅ [Silver] Delta Lake transaction committed at {SILVER_DIR} (Batch: {batch_id})")

# -------------------------------------------------------------
# 5. GOLD LAYER (Curated OLAP Mart via DuckDB Engine)
# -------------------------------------------------------------
def curate_gold() -> None:
    print("⏳ [Gold] Materializing analytical mart via DuckDB SQL...")
    con = duckdb.connect()

    gold_query = f"""
    WITH ranked_regions AS (
        SELECT 
            short_name,
            region_name,
            forecast_gco2_kwh,
            carbon_index,
            renewable_share_perc,
            fossil_share_perc,
            wind_perc,
            solar_perc,
            gas_perc,
            from_time,
            to_time,
            ROW_NUMBER() OVER (ORDER BY forecast_gco2_kwh ASC) as cleanliness_rank
        FROM delta_scan('{SILVER_DIR}')
        WHERE from_time = (SELECT MAX(from_time) FROM delta_scan('{SILVER_DIR}'))
    )
    SELECT 
        cleanliness_rank,
        short_name as region,
        forecast_gco2_kwh,
        carbon_index,
        renewable_share_perc,
        gas_perc,
        CASE 
            WHEN forecast_gco2_kwh < 100 THEN 'Optimal: Run Batch Compute Now'
            WHEN forecast_gco2_kwh BETWEEN 100 AND 200 THEN 'Moderate: Defer Optional Workloads'
            ELSE 'Peak Fossil: Avoid High-Compute Tasks'
        END AS compute_scheduling_guidance
    FROM ranked_regions
    ORDER BY cleanliness_rank ASC;
    """

    gold_df = con.execute(gold_query).df()
    gold_output = os.path.join(GOLD_DIR, "green_compute_decision_mart.parquet")
    gold_df.to_parquet(gold_output, index=False)
    
    print(f"✅ [Gold] Production Mart materialized to: {gold_output}")
    print("\n" + gold_df.head(5).to_string(index=False) + "\n")
    con.close()

if __name__ == "__main__":
    raw_data = ingest_bronze()
    transform_silver(raw_data)
    curate_gold()