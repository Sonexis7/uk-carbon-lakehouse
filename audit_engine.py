import os
import pandas as pd
import duckdb

SILVER_TABLE = "./lakehouse/silver/fact_regional_carbon".replace("\\", "/")

def generate_mock_client_logs() -> pd.DataFrame:
    """Simulates a tech company's 7-day batch compute runs (e.g., ETL / ML jobs)."""
    data = [
        {"job_name": "daily_customer_churn_model", "timestamp": "2026-10-14 14:30:00", "duration_hrs": 3.5, "server_kw": 45.0, "actual_price_per_kwh": 0.28},
        {"job_name": "finance_reconciliation_pipeline", "timestamp": "2026-10-14 17:00:00", "duration_hrs": 2.0, "server_kw": 30.0, "actual_price_per_kwh": 0.32},
        {"job_name": "data_lakehouse_vacuum_silver", "timestamp": "2026-10-14 19:15:00", "duration_hrs": 4.0, "server_kw": 50.0, "actual_price_per_kwh": 0.29},
        {"job_name": "search_embeddings_indexing", "timestamp": "2026-10-14 15:45:00", "duration_hrs": 5.0, "server_kw": 60.0, "actual_price_per_kwh": 0.30},
    ]
    return pd.DataFrame(data)

def run_financial_audit(client_logs: pd.DataFrame, region_name: str = "Midlands"):
    con = duckdb.connect()

    # Query your live Silver Delta table for the cheapest/cleanest baseline
    grid_summary = con.execute(f"""
        SELECT 
            MIN(forecast_gco2_kwh) as optimal_carbon_g,
            AVG(forecast_gco2_kwh) as avg_carbon_g
        FROM delta_scan('{SILVER_TABLE}')
        WHERE short_name LIKE '%{region_name}%' OR region_name LIKE '%{region_name}%'
    """).df()

    # Typical off-peak wind tariff benchmark on the UK wholesale grid
    optimal_tariff_kwh = 0.09  # 9p per kWh during peak wind/solar troughs

    client_logs["kwh_used"] = client_logs["duration_hrs"] * client_logs["server_kw"]
    client_logs["actual_cost_gbp"] = client_logs["kwh_used"] * client_logs["actual_price_per_kwh"]
    client_logs["optimal_cost_gbp"] = client_logs["kwh_used"] * optimal_tariff_kwh
    client_logs["waste_gbp"] = client_logs["actual_cost_gbp"] - client_logs["optimal_cost_gbp"]

    total_actual = client_logs["actual_cost_gbp"].sum()
    total_optimal = client_logs["optimal_cost_gbp"].sum()
    total_waste_7days = client_logs["waste_gbp"].sum()
    projected_annual_waste = total_waste_7days * 52

    # Executive Output Table
    print("\n" + "=" * 65)
    print(" ⚡ GREENOPS COMPUTE AUDIT: EXECUTIVE SUMMARY")
    print("=" * 65)
    print(f"Workloads Audited:            {len(client_logs)} Batch Pipelines")
    print(f"Total Electricity Consumed:    {client_logs['kwh_used'].sum():,.1f} kWh")
    print(f"Current Spend (Peak Pricing): £{total_actual:,.2f}")
    print(f"Optimized Spend (Green Grid): £{total_optimal:,.2f}")
    print("-" * 65)
    print(f"Direct Cash Wasted (7 Days):   £{total_waste_7days:,.2f}")
    print(f"PROJECTED ANNUAL AVOIDABLE LOSS: £{projected_annual_waste:,.2f}")
    print("=" * 65 + "\n")

    return client_logs

if __name__ == "__main__":
    logs = generate_mock_client_logs()
    run_financial_audit(logs)