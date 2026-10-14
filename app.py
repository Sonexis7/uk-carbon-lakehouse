import os
import streamlit as st
import pandas as pd
import duckdb

st.set_page_config(page_title="UK Grid Clean Compute Lakehouse", layout="wide")

st.title("⚡ UK National Grid: Clean Compute & Carbon Lakehouse")
st.caption("Local Medallion Architecture (Bronze -> Silver -> Gold) powered by Delta Lake & DuckDB")

GOLD_FILE = "./lakehouse/gold/green_compute_decision_mart.parquet"
SILVER_TABLE = "./lakehouse/silver/fact_regional_carbon".replace("\\", "/")

if not os.path.exists(GOLD_FILE):
    st.error("⚠️ Gold data mart not found. Please run `python pipeline.py` first.")
    st.stop()

gold_df = pd.read_parquet(GOLD_FILE)

# Metric summary cards
cleanest = gold_df.iloc[0]
dirtiest = gold_df.iloc[-1]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Cleanest Region", cleanest["region"])
col2.metric("Lowest Intensity", f"{cleanest['forecast_gco2_kwh']} gCO2/kWh")
col3.metric("Highest Renewable %", f"{cleanest['renewable_share_perc']:.1f}%")
col4.metric("Highest Carbon Region", f"{dirtiest['region']} ({dirtiest['forecast_gco2_kwh']} g)")

st.divider()

left_col, right_col = st.columns([6, 4])

with left_col:
    st.subheader("Regional Carbon Intensity (gCO2/kWh)")
    chart_data = gold_df.set_index("region")[["forecast_gco2_kwh"]].sort_values("forecast_gco2_kwh")
    st.bar_chart(chart_data)

with right_col:
    st.subheader("Compute Dispatch Guidance")
    st.dataframe(
        gold_df[["cleanliness_rank", "region", "carbon_index", "compute_scheduling_guidance"]],
        hide_index=True,
        use_container_width=True
    )

st.divider()

# Live Ad-Hoc SQL Querying against Delta Lake Storage Layer
st.subheader("🔍 Deep Dive: Live Query on Silver Delta Lake (ACID Layer)")
con = duckdb.connect()
silver_sample = con.execute(f"""
    SELECT 
        short_name as region, 
        wind_perc, 
        solar_perc, 
        nuclear_perc, 
        gas_perc, 
        fossil_share_perc, 
        _ingested_at,
        _batch_id
    FROM delta_scan('{SILVER_TABLE}')
    ORDER BY wind_perc DESC
    LIMIT 10;
""").df()
con.close()

st.dataframe(silver_sample, hide_index=True, use_container_width=True)

st.divider()

# -------------------------------------------------------------
# ROI / FINANCIAL WASTE SIMULATOR (Commercial Value Layer)
# -------------------------------------------------------------
st.subheader("💰 Enterprise GreenOps ROI Simulator")
st.caption("Calculate potential annual electricity and carbon savings by shifting batch compute to green windows.")

sim_col1, sim_col2, sim_col3 = st.columns(3)

with sim_col1:
    server_kw = st.slider("Average Server / Cluster Power Draw (kW)", min_value=10, max_value=500, value=75, step=5)
with sim_col2:
    batch_hours = st.slider("Daily Batch Runtime (Hours / Day)", min_value=1, max_value=24, value=6, step=1)
with sim_col3:
    peak_tariff = st.number_input("Current Peak Power Rate (£/kWh)", min_value=0.15, max_value=0.50, value=0.29, step=0.01)

# Financial Arbitrage Calculations
green_tariff = 0.09  # Benchmark off-peak renewable tariff
daily_kwh = server_kw * batch_hours
annual_kwh = daily_kwh * 365

annual_current_cost = annual_kwh * peak_tariff
annual_green_cost = annual_kwh * green_tariff
annual_savings = annual_current_cost - annual_green_cost

metric_col1, metric_col2, metric_col3 = st.columns(3)
metric_col1.metric("Annual Power Consumption", f"{annual_kwh:,.0f} kWh")
metric_col2.metric("Current Annual Compute Spend", f"£{annual_current_cost:,.2f}")
metric_col3.metric(
    "Projected Annual Net Savings", 
    f"£{annual_savings:,.2f}", 
    delta=f"-{((annual_savings / annual_current_cost) * 100):.1f}% Cost Reduction"
)