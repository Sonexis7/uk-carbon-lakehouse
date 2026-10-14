import os
import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go

# -----------------------------------------------------------------------------
# 1. PAGE CONFIGURATION & ENTERPRISE DARK CSS
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="UK National Grid | GreenOps Telemetry Lakehouse",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', sans-serif;
    }
    
    code, pre {
        font-family: 'JetBrains Mono', monospace !important;
    }

    /* Metric Card Containers */
    .metric-card {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.8) 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 14px;
        padding: 20px 24px;
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.3);
        backdrop-filter: blur(12px);
        margin-bottom: 15px;
    }
    
    .metric-label {
        font-size: 0.82rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94a3b8;
        margin-bottom: 6px;
    }
    
    .metric-value {
        font-size: 1.85rem;
        font-weight: 800;
        color: #f8fafc;
        line-height: 1.2;
    }

    .metric-sub {
        font-size: 0.82rem;
        color: #38bdf8;
        margin-top: 4px;
        font-weight: 600;
    }

    .badge-optimal {
        background-color: rgba(16, 185, 129, 0.15);
        color: #34d399;
        border: 1px solid rgba(16, 185, 129, 0.3);
        padding: 4px 10px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. UK DNO REGIONAL GEOLOCATION COORDINATE ENGINE
# -----------------------------------------------------------------------------
UK_DNO_COORDINATES = {
    "North Scotland": {"lat": 57.50, "lon": -4.25},
    "South Scotland": {"lat": 55.60, "lon": -3.50},
    "North East England": {"lat": 54.95, "lon": -1.60},
    "North West England": {"lat": 53.80, "lon": -2.70},
    "Yorkshire": {"lat": 53.95, "lon": -1.10},
    "East Midlands": {"lat": 52.85, "lon": -0.95},
    "West Midlands": {"lat": 52.48, "lon": -1.90},
    "East England": {"lat": 52.20, "lon": 0.45},
    "London": {"lat": 51.50, "lon": -0.12},
    "South East England": {"lat": 51.20, "lon": 0.50},
    "South England": {"lat": 51.00, "lon": -1.30},
    "South West England": {"lat": 50.70, "lon": -3.50},
    "North Wales & Mersey": {"lat": 53.20, "lon": -3.20},
    "South Wales": {"lat": 51.65, "lon": -3.50},
    "Wales": {"lat": 52.13, "lon": -3.78},
    "Scotland": {"lat": 56.49, "lon": -4.20},
    "England": {"lat": 52.35, "lon": -1.17},
}

# -----------------------------------------------------------------------------
# 3. DATA INGESTION & CACHED LOAD
# -----------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_lakehouse_data():
    db_path = "./lakehouse/gold/analytics.duckdb"
    parquet_path = "./lakehouse/gold/green_compute_decision_mart.parquet"
    
    con = duckdb.connect()
    
    # Query dbt-governed table first
    if os.path.exists(db_path):
        try:
            df = con.execute(f"ATTACH '{db_path}' AS dbt_db; SELECT * FROM dbt_db.mart_green_compute_dispatch;").df()
            con.close()
            return df, "dbt-DuckDB Mart (ACID Verified)"
        except Exception:
            pass
            
    # Fallback to direct Parquet
    if os.path.exists(parquet_path):
        df = con.execute(f"""
            SELECT 
                region, 
                forecast_gco2_kwh, 
                carbon_index, 
                renewable_share_perc, 
                cleanliness_rank, 
                compute_scheduling_guidance,
                CASE 
                    WHEN forecast_gco2_kwh <= 80 THEN 'OPTIMAL'
                    WHEN forecast_gco2_kwh <= 150 THEN 'STANDARD'
                    ELSE 'DEFER'
                END AS dispatch_tier
            FROM read_parquet('{parquet_path}')
        """).df()
        con.close()
        return df, "Gold Data Mart (Parquet)"
        
    con.close()
    return None, "No Data"

gold_df, source_label = load_lakehouse_data()

if gold_df is None:
    st.error("⚠️ Data Mart not found. Please run `python pipeline.py` or `dbt run` first.")
    st.stop()

# Enrich DataFrame with latitude and longitude
gold_df["lat"] = gold_df["region"].map(lambda r: UK_DNO_COORDINATES.get(r, {}).get("lat", None))
gold_df["lon"] = gold_df["region"].map(lambda r: UK_DNO_COORDINATES.get(r, {}).get("lon", None))

# Filter out national aggregates (e.g., 'GB') to keep regional map points clean
map_df = gold_df.dropna(subset=["lat", "lon"]).copy()
map_df["bubble_size"] = map_df["forecast_gco2_kwh"].apply(lambda x: max(16, min(x / 4, 45)))

# -----------------------------------------------------------------------------
# 4. HEADER
# -----------------------------------------------------------------------------
header_col1, header_col2 = st.columns([8, 2])
with header_col1:
    st.title("⚡ UK National Grid | GreenOps Telemetry Lakehouse")
    st.caption(f"Enterprise Medallion Architecture (Bronze ➔ Silver ➔ Gold) • Active Data Source: **{source_label}**")

with header_col2:
    st.markdown("<div style='text-align: right; padding-top: 15px;'>", unsafe_allow_html=True)
    st.markdown('<span class="badge-optimal">● TELEMETRY LIVE</span>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 5. EXECUTIVE KPI BANNER
# -----------------------------------------------------------------------------
cleanest = gold_df.sort_values("forecast_gco2_kwh").iloc[0]
dirtiest = gold_df.sort_values("forecast_gco2_kwh", ascending=False).iloc[0]
avg_intensity = gold_df["forecast_gco2_kwh"].mean()
optimal_count = len(gold_df[gold_df["dispatch_tier"] == "OPTIMAL"])

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Cleanest Dispatch Zone</div>
        <div class="metric-value">{cleanest['region']}</div>
        <div class="metric-sub">🌱 {cleanest['forecast_gco2_kwh']} gCO₂/kWh ({cleanest.get('renewable_share_perc', 100):.1f}% Clean)</div>
    </div>
    """, unsafe_allow_html=True)

with kpi2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Highest Carbon Zone</div>
        <div class="metric-value">{dirtiest['region']}</div>
        <div class="metric-sub" style="color: #f87171;">⚠️ {dirtiest['forecast_gco2_kwh']} gCO₂/kWh (Peak Fossil)</div>
    </div>
    """, unsafe_allow_html=True)

with kpi3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Grid Intensity Spread</div>
        <div class="metric-value">{dirtiest['forecast_gco2_kwh'] - cleanest['forecast_gco2_kwh']:.0f} <span style="font-size:1.1rem; color:#94a3b8;">gCO₂</span></div>
        <div class="metric-sub" style="color: #fbbf24;">⚡ National Avg: {avg_intensity:.1f} gCO₂/kWh</div>
    </div>
    """, unsafe_allow_html=True)

with kpi4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Optimal Dispatch Windows</div>
        <div class="metric-value">{optimal_count} <span style="font-size:1.1rem; color:#94a3b8;">/ {len(gold_df)} Regions</span></div>
        <div class="metric-sub">🚀 Low Carbon & Tariffs Active</div>
    </div>
    """, unsafe_allow_html=True)

st.write("")

# -----------------------------------------------------------------------------
# 6. TABBED INTERFACE (MAP + CHARTS + GOVERNANCE + SIMULATOR)
# -----------------------------------------------------------------------------
tab_overview, tab_governance, tab_simulator = st.tabs([
    "🗺️ Real-Time Grid Telemetry & Map", 
    "🛡️ dbt Data Governance & Lineage", 
    "💰 Enterprise GreenOps ROI Modeler"
])

with tab_overview:
    # 2-Column Command Center: Geospatial Map on Left, Spectrum Chart on Right
    map_col, bar_col = st.columns([5.5, 4.5])
    
    with map_col:
        st.subheader("Interactive UK Grid Carbon Map")
        
        # Adaptive Plotly Map: supports both Plotly v6+ (scatter_map) and legacy v5 (scatter_mapbox)
        map_func = getattr(px, "scatter_map", None) or getattr(px, "scatter_mapbox")
        style_key = "map_style" if hasattr(px, "scatter_map") else "mapbox_style"
        
        fig_map = map_func(
            map_df,
            lat="lat",
            lon="lon",
            hover_name="region",
            size="bubble_size",
            color="forecast_gco2_kwh",
            color_continuous_scale=[
                (0.00, "#10b981"),  # Emerald Green (Zero/Low Carbon)
                (0.30, "#06b6d4"),  # Cyan
                (0.55, "#f59e0b"),  # Amber
                (1.00, "#ef4444")   # Crimson (Peak Fossil Gas)
            ],
            hover_data={
                "forecast_gco2_kwh": ":.0f",
                "dispatch_tier": True,
                "cleanliness_rank": True,
                "lat": False,
                "lon": False,
                "bubble_size": False
            },
            zoom=5.0,
            center={"lat": 54.8, "lon": -2.8},
            **{style_key: "carto-darkmatter"},
            height=580
        )
        
        fig_map.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar=dict(
                title=dict(text="gCO₂/kWh", font=dict(color="#94a3b8", size=11)),
                tickfont=dict(color="#94a3b8"),
                len=0.75,
                thickness=12,
                x=0.02,
                y=0.5
            )
        )
        st.plotly_chart(fig_map, use_container_width=True)

    with bar_col:
        st.subheader("Regional Carbon Spectrum")
        sorted_df = gold_df.sort_values("forecast_gco2_kwh", ascending=True)
        
        fig_bar = px.bar(
            sorted_df,
            x="forecast_gco2_kwh",
            y="region",
            orientation="h",
            color="forecast_gco2_kwh",
            color_continuous_scale=[
                (0.00, "#10b981"),
                (0.30, "#06b6d4"),
                (0.55, "#f59e0b"),
                (1.00, "#ef4444")
            ],
            text="forecast_gco2_kwh",
            labels={"forecast_gco2_kwh": "gCO₂/kWh", "region": ""}
        )
        
        fig_bar.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#94a3b8", family="Plus Jakarta Sans"),
            height=580,
            margin=dict(l=10, r=20, t=0, b=0),
            coloraxis_showscale=False,
            xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.06)", zeroline=False),
            yaxis=dict(showgrid=False)
        )
        fig_bar.update_traces(
            texttemplate="%{text:.0f} g",
            textposition="outside",
            marker_line_width=0,
            opacity=0.95
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.divider()

    # Routing Matrix Below
    st.subheader("Compute Dispatch Routing Matrix")
    display_cols = ["region", "forecast_gco2_kwh", "dispatch_tier", "cleanliness_rank"]
    styled_table = gold_df[display_cols].sort_values("cleanliness_rank")
    
    st.dataframe(
        styled_table,
        column_config={
            "region": st.column_config.TextColumn("DNO Region", width="medium"),
            "forecast_gco2_kwh": st.column_config.ProgressColumn(
                "Carbon Intensity",
                help="gCO2/kWh emissions",
                format="%d g",
                min_value=0,
                max_value=int(dirtiest["forecast_gco2_kwh"]) + 10,
            ),
            "dispatch_tier": st.column_config.TextColumn("Guidance Tier", width="small"),
            "cleanliness_rank": st.column_config.NumberColumn("Rank", width="small")
        },
        hide_index=True,
        use_container_width=True
    )

# -----------------------------------------------------------------------------
# 7. DBT DATA GOVERNANCE & ACID LINEAGE TAB
# -----------------------------------------------------------------------------
with tab_governance:
    st.subheader("Governed Silver Delta Lake Storage Layer")
    st.caption("Live, zero-copy query directly against partitioned ACID Delta Parquet logs.")
    
    con = duckdb.connect()
    silver_path = "./lakehouse/silver/fact_regional_carbon".replace("\\", "/")
    
    try:
        silver_data = con.execute(f"""
            SELECT 
                short_name as region,
                wind_perc as "Wind %",
                solar_perc as "Solar %",
                nuclear_perc as "Nuclear %",
                gas_perc as "Gas %",
                fossil_share_perc as "Fossil Total %",
                _ingested_at,
                _batch_id
            FROM delta_scan('{silver_path}')
            ORDER BY wind_perc DESC
            LIMIT 14;
        """).df()
        
        st.dataframe(silver_data, hide_index=True, use_container_width=True)
    except Exception as e:
        st.info("Silver Delta Lake layer pending write: " + str(e))
    finally:
        con.close()
        
    st.markdown("""
    **Enterprise Governance Contracts Implemented:**
    * ✅ **dbt Core:** Declarative schema contracts enforcing primary key uniqueness on `region_id`.
    * ✅ **Physical Quality Gates:** Generation mix asserts ($\sum \% \le 105\%$) preventing tainted downstream marts.
    * ✅ **Lineage Tracking:** Immutable `_batch_id` and `_ingested_at` audit columns on every Delta record.
    """)

# -----------------------------------------------------------------------------
# 8. ENTERPRISE GREENOPS ROI ARBITRAGE SIMULATOR
# -----------------------------------------------------------------------------
with tab_simulator:
    st.subheader("💰 B2B Cloud Compute Waste & ROI Modeler")
    st.caption("Quantifying annual electricity tariff arbitrage by dynamically shifting batch workloads out of peak fossil hours.")
    
    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns(3)
    with ctrl_col1:
        sim_kw = st.slider("Server / Cluster Power Draw (kW)", min_value=10, max_value=500, value=75, step=5)
    with ctrl_col2:
        sim_hours = st.slider("Daily Batch Runtime (Hours / Day)", min_value=1, max_value=24, value=6, step=1)
    with ctrl_col3:
        sim_peak = st.number_input("Peak Tariff (£/kWh)", min_value=0.15, max_value=0.50, value=0.29, step=0.01)
        
    sim_green = 0.09  # Benchmark off-peak renewable wind tariff
    annual_kwh = sim_kw * sim_hours * 365
    annual_unopt_cost = annual_kwh * sim_peak
    annual_opt_cost = annual_kwh * sim_green
    annual_net_savings = annual_unopt_cost - annual_opt_cost
    pct_reduction = (annual_net_savings / annual_unopt_cost) * 100
    
    st.divider()
    
    res_col1, res_col2 = st.columns([4, 6])
    
    with res_col1:
        st.markdown(f"""
        <div class="metric-card" style="border-left: 5px solid #10b981;">
            <div class="metric-label">Projected Annual Savings</div>
            <div class="metric-value" style="color: #34d399;">£{annual_net_savings:,.2f}</div>
            <div class="metric-sub">📉 -{pct_reduction:.1f}% Direct Cloud/Power Reduction</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Annual Unoptimized Spend</div>
            <div class="metric-value" style="color: #f87171;">£{annual_unopt_cost:,.2f}</div>
            <div class="metric-sub" style="color: #94a3b8;">{annual_kwh:,.0f} kWh consumed / year</div>
        </div>
        """, unsafe_allow_html=True)

    with res_col2:
        cost_df = pd.DataFrame({
            "Scenario": ["Current (Peak Fossil Windows)", "GreenOps Optimized (Wind Windows)"],
            "Annual Spend (£)": [annual_unopt_cost, annual_opt_cost],
            "Color": ["#ef4444", "#10b981"]
        })
        
        fig_roi = px.bar(
            cost_df,
            x="Scenario",
            y="Annual Spend (£)",
            color="Scenario",
            color_discrete_map={
                "Current (Peak Fossil Windows)": "#ef4444",
                "GreenOps Optimized (Wind Windows)": "#10b981"
            },
            text="Annual Spend (£)"
        )
        fig_roi.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#94a3b8", family="Plus Jakarta Sans"),
            height=280,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.06)", zeroline=False),
            xaxis=dict(showgrid=False)
        )
        fig_roi.update_traces(texttemplate="£%{text:,.2f}", textposition="outside")
        st.plotly_chart(fig_roi, use_container_width=True)

# -----------------------------------------------------------------------------
# 9. FOOTER
# -----------------------------------------------------------------------------
st.divider()
st.markdown("""
<div style="text-align: center; color: #64748b; font-size: 0.85rem;">
    UK National Grid GreenOps Lakehouse • Architecture mirrors Microsoft Fabric OneLake & Delta format • Built with DuckDB, Delta Lake, dbt & Plotly
</div>
""", unsafe_allow_html=True)