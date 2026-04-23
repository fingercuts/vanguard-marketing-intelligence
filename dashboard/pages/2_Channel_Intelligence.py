"""
Vanguard Marketing Intelligence — Cross-Channel Intelligence.

Strategic comparative analysis of marketing networks:
- Global Efficiency Benchmarking
- Investment Distribution (CapEx Allocation)
- Hourly Interaction Density (Heatmapping)
- Temporal Velocity Trends
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
    bar_chart, line_chart, heatmap_chart, pie_chart
)
from dashboard.components.filters import render_sidebar_filters, apply_filters

# ---------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------
st.set_page_config(
    page_title="Channel Intelligence | Vanguard",
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
        background-image: radial-gradient(circle at 90% 10%, rgba(128, 0, 0, 0.02) 0%, transparent 30%);
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
    
    div[data-testid="stDataFrame"] {{
        border: 1px solid {COLORS['border']};
        border-radius: 12px;
        background: {COLORS['surface']};
    }}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------
# Data Access Layer
# ---------------------------------------------------------------
@st.cache_data(ttl=300)
def load_performance_data():
    """Retrieves channel-specific performance telemetry."""
    db_path = Path(__file__).resolve().parent.parent.parent / "db" / "vanguard_intelligence.duckdb"
    conn = duckdb.connect(str(db_path), read_only=True)
    df_channel = conn.execute("SELECT * FROM gold_channel_daily").fetchdf()
    df_hourly = conn.execute("SELECT * FROM gold_hourly_trends").fetchdf()
    conn.close()
    return df_channel, df_hourly


# ---------------------------------------------------------------
# Main Interface
# ---------------------------------------------------------------
def main():
    df_channel, df_hourly = load_performance_data()

    if df_channel.empty:
        st.warning("SYSTEM ALERT: Channel aggregate telemetry unavailable.")
        return

    # Sidebar: Governance Filters
    filters = render_sidebar_filters(df_channel, show_campaign=False)
    df = apply_filters(df_channel, filters)

    # ---- Executive Header ----
    st.markdown(f"""
    <div style="padding: 10px 0px 30px 0px; margin-bottom: 20px;">
        <h1 style="font-size: 32px; font-weight: 800; color: {COLORS['text']}; margin: 0 0 8px 0; letter-spacing: -1px; text-transform: uppercase;">
            Cross-Channel Intelligence
        </h1>
        <p style="color: {COLORS['text_muted']}; font-size: 15px; font-weight: 400; margin: 0;">
            Comparative benchmarking of advertising ecosystems and budget allocation efficiency.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Aggregate Network Summary
    channel_summary = df.groupby("channel").agg(
        total_revenue=("total_revenue", "sum"),
        total_spend=("total_spend", "sum"),
        total_clicks=("total_clicks", "sum"),
        total_impressions=("total_impressions", "sum"),
        total_conversions=("total_conversions", "sum"),
    ).reset_index()
    
    channel_summary["roas"] = (channel_summary["total_revenue"] / channel_summary["total_spend"]).round(2)
    channel_summary["ctr"] = (channel_summary["total_clicks"] / channel_summary["total_impressions"]).round(4)
    channel_summary["cpc"] = (channel_summary["total_spend"] / channel_summary["total_clicks"]).round(2)

    # Headline Cluster
    kpi_list = []
    for _, row in channel_summary.iterrows():
        kpi_list.append({
            "label": f"{row['channel']} Performance",
            "value": f"${row['total_revenue']:,.0f}",
            "icon": None,
            "delta": f"ROAS {row['roas']:.2f}x",
        })
    render_kpi_cards(kpi_list)

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Governance Audit: Channel Comparison ----
    render_section_header("Network Performance Benchmark", "Consolidated performance metrics by advertising primary")

    display_df = channel_summary[["channel", "total_revenue", "total_spend", "roas",
                                    "total_conversions", "ctr", "cpc"]].copy()
    display_df.columns = ["NETWORK", "REVENUE ($)", "SPEND ($)", "ROAS",
                            "CONVERSIONS", "CTR", "CPC ($)"]
    
    st.dataframe(
        display_df.style.format({
            "REVENUE ($)": "${:,.0f}",
            "SPEND ($)": "${:,.0f}",
            "ROAS": "{:.2f}x",
            "CTR": "{:.2%}",
            "CPC ($)": "${:.2f}",
        }),
        use_container_width=True,
    )

    # ---- Competitive Analysis Grid ----
    col_a, col_b = st.columns(2)

    with col_a:
        render_section_header("Efficiency Yield by Network", "Comparative ROAS across ecosystems")
        fig = bar_chart(channel_summary.sort_values("roas", ascending=False),
                         x="channel", y="roas", color="channel", title="")
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        render_section_header("Capital Allocation Share", "Distribution of advertising budget")
        fig = pie_chart(channel_summary, values="total_spend", names="channel")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Channel Velocity Trend ----
    render_section_header("Attribution Velocity", "Daily revenue contribution by primary network")

    daily_channel = df.groupby(["date", "channel"]).agg(
        total_revenue=("total_revenue", "sum")
    ).reset_index()

    fig = line_chart(daily_channel, x="date", y="total_revenue", color="channel", title="")
    fig.update_layout(height=420)
    st.plotly_chart(fig, use_container_width=True)

    # ---- Spatial Density Analysis ----
    render_section_header("Temporal Interaction Density", "Operational heatmapping of click-through efficiency by hour")

    if not df_hourly.empty:
        # Governance: Filter based on global sidebar context
        if "channels" in filters:
            df_hourly_filtered = df_hourly[df_hourly["channel"].isin(filters["channels"])]
        else:
            df_hourly_filtered = df_hourly

        hourly_agg = df_hourly_filtered.groupby(["hour", "channel"]).agg(
            avg_ctr=("avg_ctr", "mean")
        ).reset_index()

        fig = heatmap_chart(hourly_agg, x="hour", y="channel", z="avg_ctr",
                             title="")
        fig.update_layout(height=320)
        fig.update_xaxes(title="Operating Hour (UTC)")
        fig.update_yaxes(title="Network")
        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
