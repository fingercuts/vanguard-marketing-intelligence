"""
Vanguard Marketing Intelligence — Executive Overview (Central Command).

Enterprise-grade analytical command center providing:
- High-fidelity performance telemetry (Revenue, Efficiency, Spend)
- Cross-channel trend analysis and budget allocation
- ROI-driven campaign performance ranking
"""

import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import COLORS, DUCKDB_PATH
from dashboard.components.kpi_cards import render_kpi_cards, render_section_header
from dashboard.components.charts import (
    line_chart, pie_chart, dual_axis_chart, area_chart
)
from dashboard.components.filters import render_sidebar_filters, apply_filters

# ---------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------
st.set_page_config(
    page_title="Vanguard Marketing Intelligence",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------
# Premium Interface Design System
# ---------------------------------------------------------------
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Inter:wght@400;500;600&display=swap');
    
    * {{
        font-family: 'Outfit', 'Inter', sans-serif;
    }}
    
    /* Background and Canvas Optimization */
    .stApp {{
        background-color: {COLORS['background']};
        background-image: radial-gradient(circle at 20% 10%, rgba(128, 0, 0, 0.03) 0%, transparent 40%),
                          radial-gradient(circle at 80% 80%, rgba(15, 23, 42, 0.03) 0%, transparent 40%);
    }}
    
    /* Executive Sidebar Styling */
    section[data-testid="stSidebar"] {{
        background-color: {COLORS['surface']};
        border-right: 1px solid {COLORS['border']};
        backdrop-filter: blur(15px);
    }}
    
    section[data-testid="stSidebar"] .st-emotion-cache-16txtl3 {{
        padding-top: 3rem;
    }}
    
    /* Hide Streamlit Native Branding */
    header, #MainMenu, footer {{
        visibility: hidden !important;
    }}
    
    /* Typography & Header Polish */
    h1, h2, h3 {{
        color: {COLORS['text']} !important;
        letter-spacing: -0.02em;
    }}
    
    .stMetric label {{
        color: {COLORS['text_muted']} !important;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        font-size: 0.8rem;
    }}
    
    /* Component Glassmorphism */
    div[data-testid="stDataFrame"] {{
        border: 1px solid {COLORS['border']};
        border-radius: 12px;
        background: {COLORS['surface']};
        backdrop-filter: blur(10px);
    }}
    
    .stTabs [data-baseweb="tab-list"] {{
        gap: 12px;
        background-color: transparent;
    }}
    
    .stTabs [data-baseweb="tab"] {{
        background-color: {COLORS['surface']};
        padding: 10px 20px;
        border-radius: 10px;
        color: {COLORS['text_muted']};
        border: 1px solid {COLORS['border']};
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }}
    
    .stTabs [aria-selected="true"] {{
        background-color: {COLORS['primary']} !important;
        color: white !important;
        box-shadow: 0 4px 12px rgba(128, 0, 0, 0.2);
    }}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------
# Data Ingestion Layer
# ---------------------------------------------------------------
@st.cache_data(ttl=300)
def load_performance_data():
    """Retrieves high-fidelity analytics data from the Gold layer."""
    db_path = Path(__file__).resolve().parent.parent / "db" / "vanguard_intelligence.duckdb"
    conn = duckdb.connect(str(db_path), read_only=True)
    
    data = {}
    tables = ["gold_campaign_daily", "gold_channel_daily", "gold_hourly_trends", "gold_funnel_summary"]
    
    for table in tables:
        try:
            data[table] = conn.execute(f"SELECT * FROM {table}").fetchdf()
        except Exception:
            data[table] = pd.DataFrame()
    
    conn.close()
    return data


# ---------------------------------------------------------------
# Application Mainframe
# ---------------------------------------------------------------
def main():
    data = load_performance_data()
    df_campaign = data["gold_campaign_daily"]
    df_channel = data["gold_channel_daily"]

    if df_campaign.empty:
        st.warning("SYSTEM ALERT: No processed data found. Ensure the pipeline execution is complete.")
        return

    # Sidebar: Governance Filters
    filters = render_sidebar_filters(df_campaign)
    df_filtered = apply_filters(df_campaign, filters)
    df_channel_filtered = apply_filters(df_channel, filters)

    # ---- Executive Header ----
    st.markdown(f"""
    <div style="padding: 20px 0px 40px 0px; margin-bottom: 20px;">
        <div style="display: flex; align-items: center; margin-bottom: 8px;">
            <div style="width: 32px; height: 32px; background: {COLORS['primary']}; border-radius: 6px; margin-right: 12px; display: flex; align-items: center; justify-content: center; color: white; font-weight: 800; font-size: 14px;">V</div>
            <h1 style="font-size: 34px; font-weight: 800; color: {COLORS['text']}; margin: 0; letter-spacing: -1.5px; text-transform: uppercase;">
                Vanguard Marketing Intelligence
            </h1>
        </div>
        <p style="color: {COLORS['text_muted']}; font-size: 15px; font-weight: 400; margin: 0; padding-left: 44px;">
            Unified analytical command for global channel performance and budget efficiency.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ---- Headline Performance Metrics ----
    total_rev = df_filtered["total_revenue"].sum()
    total_spend = df_filtered["total_spend"].sum()
    total_conv = df_filtered["total_conversions"].sum()
    total_clicks = df_filtered["total_clicks"].sum()
    total_imp = df_filtered["total_impressions"].sum()
    
    roas = total_rev / total_spend if total_spend > 0 else 0
    ctr = total_clicks / total_imp if total_imp > 0 else 0
    cpa = total_spend / total_conv if total_conv > 0 else 0

    render_kpi_cards([
        {"label": "Total Revenue", "value": f"${total_rev:,.0f}", "icon": None,
         "delta": f"Efficiency {roas:.2f}x"},
        {"label": "Ad Spend", "value": f"${total_spend:,.0f}", "icon": None,
         "delta": f"{len(df_filtered['campaign_id'].unique())} active campaigns"},
        {"label": "Return on Ad Spend", "value": f"{roas:.2f}x", "icon": None,
         "delta": "HEALTHY" if roas > 2.5 else "REBALANCING NEEDED"},
        {"label": "Conversions", "value": f"{total_conv:,}", "icon": None,
         "delta": f"CPA ${cpa:.2f}" if total_conv > 0 else "N/A"},
        {"label": "Click-Through Rate", "value": f"{ctr:.2%}", "icon": None,
         "delta": f"{total_clicks:,} interactions"},
    ])

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Temporal Revenue Analysis ----
    render_section_header("Cross-Channel Revenue Velocity", "Daily revenue attribution tracking by network")

    if "date" in df_channel_filtered.columns and not df_channel_filtered.empty:
        daily_agg = df_channel_filtered.groupby(["date", "channel"]).agg(
            total_revenue=("total_revenue", "sum")
        ).reset_index()
        fig = area_chart(daily_agg, x="date", y="total_revenue", color="channel",
                          title="")
        fig.update_layout(height=420)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("System Notification: Insufficient data for trend visualization.")

    # ---- Segmented Analytics Grid ----
    col1, col2 = st.columns([2, 3])

    with col1:
        render_section_header("Revenue Attribution Share", "Distribution by primary advertising network")
        if not df_channel_filtered.empty:
            channel_totals = df_channel_filtered.groupby("channel").agg(
                total_revenue=("total_revenue", "sum")
            ).reset_index()
            fig = pie_chart(channel_totals, values="total_revenue", names="channel")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        render_section_header("Investment vs. Return Analysis", "Temporal correlation of capital deployment and revenue generation")
        if not df_channel_filtered.empty:
            daily_totals = df_channel_filtered.groupby("date").agg(
                total_spend=("total_spend", "sum"),
                total_revenue=("total_revenue", "sum"),
            ).reset_index()
            fig = dual_axis_chart(
                daily_totals, x="date", y1="total_spend", y2="total_revenue",
                y1_name="Investment ($)", y2_name="Revenue ($)",
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # ---- High-Performing Campaign Index ----
    render_section_header("Top Campaign Index", "Performance ranking based on core ROI benchmarks")

    if not df_filtered.empty:
        top_campaigns = (
            df_filtered.groupby(["campaign_id", "campaign_name", "channel"])
            .agg(
                total_revenue=("total_revenue", "sum"),
                total_spend=("total_spend", "sum"),
                total_conversions=("total_conversions", "sum"),
                total_clicks=("total_clicks", "sum"),
                total_impressions=("total_impressions", "sum"),
            )
            .reset_index()
        )
        top_campaigns["roas"] = (
            top_campaigns["total_revenue"] / top_campaigns["total_spend"]
        ).where(top_campaigns["total_spend"] > 0, 0).round(2)
        top_campaigns["ctr"] = (
            top_campaigns["total_clicks"] / top_campaigns["total_impressions"]
        ).where(top_campaigns["total_impressions"] > 0, 0).round(4)

        top_campaigns = top_campaigns.sort_values("total_revenue", ascending=False).head(15)
        
        display_df = top_campaigns[["campaign_id", "campaign_name", "channel",
                                      "total_revenue", "total_spend", "roas",
                                      "total_conversions", "ctr"]].copy()
        display_df.columns = ["ID", "CAMPAIGN NAME", "CHANNEL",
                               "REVENUE ($)", "SPEND ($)", "ROAS",
                               "CONVERSIONS", "CTR"]
        
        st.dataframe(
            display_df.style.format({
                "REVENUE ($)": "${:,.0f}",
                "SPEND ($)": "${:,.0f}",
                "ROAS": "{:.2f}x",
                "CTR": "{:.2%}",
            }),
            use_container_width=True,
            height=450,
        )


if __name__ == "__main__":
    main()
else:
    main()
