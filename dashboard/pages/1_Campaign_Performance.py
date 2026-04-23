"""
Vanguard Marketing Intelligence — Campaign Performance Optimization.

Provides granular campaign-level telemetry:
- Performance Benchmarking (ROAS, CPA)
- Budget Utilization Trends
- Multi-step Conversion Funnel Analysis
"""

import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import COLORS
from dashboard.components.kpi_cards import render_kpi_cards, render_section_header
from dashboard.components.charts import (
    bar_chart, line_chart, funnel_chart, dual_axis_chart
)
from dashboard.components.filters import render_sidebar_filters, apply_filters

# ---------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------
st.set_page_config(
    page_title="Campaign Intelligence | Vanguard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------------------
# Premium Design System
# ---------------------------------------------------------------
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Inter:wght@400;500;600&display=swap');
    
    * {{
        font-family: 'Outfit', 'Inter', sans-serif;
    }}
    
    .stApp {{
        background-color: {COLORS['background']};
        background-image: radial-gradient(circle at 10% 20%, rgba(128, 0, 0, 0.02) 0%, transparent 30%);
    }}
    
    section[data-testid="stSidebar"] {{
        background-color: {COLORS['surface']};
        border-right: 1px solid {COLORS['border']};
        backdrop-filter: blur(10px);
    }}
    
    header, #MainMenu, footer {{
        visibility: hidden !important;
    }}
    
    h1, h2, h3 {{
        color: {COLORS['text']} !important;
        letter-spacing: -0.01em;
    }}
    
    .stMetric label {{
        color: {COLORS['text_muted']} !important;
        text-transform: uppercase;
        font-weight: 500;
        font-size: 0.75rem;
        letter-spacing: 0.05em;
    }}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------
# Data Access Layer
# ---------------------------------------------------------------
@st.cache_data(ttl=300)
def load_performance_data():
    """Retrieves campaign and funnel telemetry from the analytical warehouse."""
    db_path = Path(__file__).resolve().parent.parent.parent / "db" / "vanguard_intelligence.duckdb"
    conn = duckdb.connect(str(db_path), read_only=True)
    df_camp = conn.execute("SELECT * FROM gold_campaign_daily").fetchdf()
    df_funnel = conn.execute("SELECT * FROM gold_funnel_summary").fetchdf()
    conn.close()
    return df_camp, df_funnel


# ---------------------------------------------------------------
# Main Interface
# ---------------------------------------------------------------
def main():
    df_campaign, df_funnel = load_performance_data()

    if df_campaign.empty:
        st.warning("SYSTEM ALERT: Primary aggregate tables empty. Verify ETL pipeline status.")
        return

    # Sidebar: Governance Filters
    filters = render_sidebar_filters(df_campaign, show_campaign=True)
    df = apply_filters(df_campaign, filters)

    # ---- Executive Header ----
    st.markdown(f"""
    <div style="padding: 10px 0px 30px 0px; margin-bottom: 20px;">
        <h1 style="font-size: 32px; font-weight: 800; color: {COLORS['text']}; margin: 0 0 8px 0; letter-spacing: -1px; text-transform: uppercase;">
            Campaign Performance Optimization
        </h1>
        <p style="color: {COLORS['text_muted']}; font-size: 15px; font-weight: 400; margin: 0;">
            Granular attribution modeling and budget efficiency analysis per active campaign.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ---- KPI Benchmarking ----
    num_camps = df["campaign_id"].nunique()
    total_rev = df["total_revenue"].sum()
    total_spend = df["total_spend"].sum()
    total_conv = df["total_conversions"].sum()
    
    avg_roas = total_rev / total_spend if total_spend > 0 else 0
    avg_cpa = total_spend / total_conv if total_conv > 0 else 0
    
    # Identify alpha campaign
    if not df.empty:
        best_camp_id = df.groupby("campaign_id")["total_revenue"].sum().idxmax()
    else:
        best_camp_id = "N/A"

    render_kpi_cards([
        {"label": "Active Campaign Count", "value": str(num_camps), "icon": None, "delta": "Across filtered scope"},
        {"label": "Aggregate ROAS", "value": f"{avg_roas:.2f}x", "icon": None,
         "delta": "TARGET MET" if avg_roas > 2.0 else "SUB-OPTIMAL"},
        {"label": "Mean Acquisition Cost", "value": f"${avg_cpa:.2f}", "icon": None, "delta": "CPA (Blended)"},
        {"label": "High-Alpha Campaign", "value": best_camp_id, "icon": None, "delta": "Primary Revenue Driver"},
    ])

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Comparative Revenue Analysis ----
    render_section_header("Revenue Integrity by Campaign", "Absolute revenue generation across channels")

    camp_totals = df.groupby(["campaign_id", "channel"]).agg(
        total_revenue=("total_revenue", "sum")
    ).reset_index().sort_values("total_revenue", ascending=False)

    fig = bar_chart(camp_totals.head(30), x="campaign_id", y="total_revenue",
                     color="channel", title="")
    fig.update_layout(height=450, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

    # ---- Efficiency Ranking Grid ----
    col_a, col_b = st.columns(2)

    with col_a:
        render_section_header("Efficiency Index (ROAS)", "Top 10 performing assets by efficiency")
        camp_roas = df.groupby("campaign_id").agg(
            total_revenue=("total_revenue", "sum"),
            total_spend=("total_spend", "sum"),
        ).reset_index()
        camp_roas["roas"] = (camp_roas["total_revenue"] / camp_roas["total_spend"]).round(2)
        camp_roas = camp_roas.sort_values("roas", ascending=True).tail(10)

        fig = bar_chart(camp_roas, x="roas", y="campaign_id", orientation="h",
                         title="")
        fig.update_layout(height=400)
        fig.update_traces(marker_color=COLORS["success"])
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        render_section_header("Acquisition Efficiency (CPA)", "Top 10 campaigns by lowest acquisition cost")
        camp_cpa = df.groupby("campaign_id").agg(
            total_spend=("total_spend", "sum"),
            total_conversions=("total_conversions", "sum"),
        ).reset_index()
        camp_cpa["cpa"] = (
            camp_cpa["total_spend"] / camp_cpa["total_conversions"]
        ).where(camp_cpa["total_conversions"] > 0, 0).round(2)
        camp_cpa = camp_cpa.sort_values("cpa", ascending=True).head(10)

        fig = bar_chart(camp_cpa, x="cpa", y="campaign_id", orientation="h",
                         title="")
        fig.update_layout(height=400)
        fig.update_traces(marker_color=COLORS["info"])
        st.plotly_chart(fig, use_container_width=True)

    # ---- Funnel Velocity ----
    render_section_header("Conversion Funnel Velocity", "Holistic lifecycle from initial impression to acquisition")

    total_imp = int(df["total_impressions"].sum())
    total_clk = int(df["total_clicks"].sum())
    total_conv = int(df["total_conversions"].sum())

    fig = funnel_chart(
        labels=["Impressions", "Clicks", "Conversions"],
        values=[total_imp, total_clk, total_conv],
        title="",
    )
    fig.update_layout(height=380)
    st.plotly_chart(fig, use_container_width=True)

    # ---- Temporal Drill-down ----
    render_section_header("Temporal Performance Audit", "Dynamic selection for daily budget correlation")

    selected_camp = st.selectbox(
        "Select Target Campaign",
        options=sorted(df["campaign_id"].unique()),
        key="camp_trend_select",
    )
    camp_daily = df[df["campaign_id"] == selected_camp].groupby("date").agg(
        total_revenue=("total_revenue", "sum"),
        total_spend=("total_spend", "sum"),
    ).reset_index()

    if not camp_daily.empty:
        fig = dual_axis_chart(
            camp_daily, x="date", y1="total_spend", y2="total_revenue",
            y1_name="Investment ($)", y2_name="Revenue ($)",
            title=f"Diagnostic: {selected_camp}"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
