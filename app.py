import streamlit as st
import pandas as pd
import duckdb

st.set_page_config(page_title="UK Grid Clean Compute Lakehouse", layout="wide")

st.title("⚡ UK National Grid: Clean Compute & Carbon Lakehouse")
st.caption("Local Medallion Architecture (Bronze -> Silver -> Gold) powered by Delta Lake & DuckDB")

GOLD_FILE = "./lakehouse/gold/green_compute_decision_mart.parquet"
SILVER_TABLE = "./lakehouse/silver/fact_regional_carbon"

try:
    gold_df = pd.read_parquet(GOLD_FILE)
except Exception:
    st.error("⚠️ Gold data mart not found. Please run `python pipeline.py` first.")
    st.stop()

# Metric summary cards
cleanest = gold_df.iloc[0]
dirtiest = gold_df.iloc[-1]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Cleanest Region", cleanest["region"])
col2.metric("Lowest Intensity", f"{cleanest['forecast_gco2_kwh']} gCO2/kWh")
col3.metric("Highest Renewable %", f"{cleanest['renewable_share_perc']:.1f}%")
col4.metric("Highest Carbon Region", f"{dirtiest['region']} ({dirtiest['forecast_gco2_kwh']} g)")

st.divider()

# Charts & Guidance
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

# Deep Dive: Querying Silver Delta Table directly with DuckDB
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