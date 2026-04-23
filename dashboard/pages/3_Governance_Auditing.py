"""
Vanguard Marketing Intelligence — Data Governance & Strategic Auditing.

High-integrity monitor for data quality protocols:
- System Performance Telemetry (Governance Score)
- Automated Validation Tracking (Completeness, Validity, Anomaly)
- Dimensional Volume Metrics (Medallion Layer Index)
- Audit Details Per Batch
"""

import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import COLORS
from dashboard.components.kpi_cards import render_kpi_cards, render_section_header

# ---------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------
st.set_page_config(
    page_title="Governance Monitor | Vanguard",
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
        background-image: radial-gradient(circle at 50% 50%, rgba(128, 0, 0, 0.01) 0%, transparent 40%);
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
    
    .stTabs [data-baseweb="tab-list"] {{
        gap: 12px;
    }}
    
    .stTabs [data-baseweb="tab"] {{
        background-color: {COLORS['surface']};
        padding: 10px 20px;
        border-radius: 10px;
        color: {COLORS['text_muted']};
        border: 1px solid {COLORS['border']};
    }}
    
    .stTabs [aria-selected="true"] {{
        background-color: {COLORS['primary']} !important;
        color: white !important;
    }}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------
# Governance Data Domain
# ---------------------------------------------------------------
@st.cache_data(ttl=300)
def load_governance_telemetry():
    """Retrieves operational audit logs from the intelligence warehouse."""
    db_path = Path(__file__).resolve().parent.parent.parent / "db" / "vanguard_intelligence.duckdb"
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        df = conn.execute("SELECT * FROM quality_check_results ORDER BY checked_at DESC").fetchdf()
    except Exception:
        df = pd.DataFrame()
    
    # Inventory Metadata
    tables = {}
    try:
        table_list = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchdf()["table_name"].tolist()
        for t in table_list:
            if t != "quality_check_results":
                count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                tables[t] = count
    except Exception:
        pass

    conn.close()
    return df, tables


# ---------------------------------------------------------------
# Main Interface
# ---------------------------------------------------------------
def main():
    df_quality, table_info = load_governance_telemetry()

    # ---- Executive Header ----
    st.markdown(f"""
    <div style="padding: 10px 0px 30px 0px; margin-bottom: 20px;">
        <h1 style="font-size: 32px; font-weight: 800; color: {COLORS['text']}; margin: 0 0 8px 0; letter-spacing: -1px; text-transform: uppercase;">
            Governance & Strategic Auditing
        </h1>
        <p style="color: {COLORS['text_muted']}; font-size: 15px; font-weight: 400; margin: 0;">
            Unified monitor for automated pipeline validation, integrity auditing, and system health telemetry.
        </p>
    </div>
    """, unsafe_allow_html=True)

    if df_quality.empty:
        st.warning("SYSTEM ALERT: No audit telemetry available. Ensure the data governance suite has been executed.")
        
        if table_info:
            render_section_header("Operational Table Index", "Medallion architecture inventory and volume markers")
            table_df = pd.DataFrame([
                {"TABLE": k, "VOLUME (ROWS)": f"{v:,}", "LAYER": 
                 "BRONZE" if k.startswith("bronze") else 
                 "SILVER" if k.startswith("silver") else 
                 "GOLD" if k.startswith("gold") else "SYSTEM"}
                for k, v in sorted(table_info.items())
            ])
            st.dataframe(table_df, use_container_width=True)
        return

    # ---- Governance Baseline KPIs ----
    total_checks = len(df_quality)
    passed = (df_quality["status"] == "PASS").sum()
    warned = (df_quality["status"] == "WARN").sum()
    failed = (df_quality["status"] == "FAIL").sum()
    pass_rate = passed / total_checks * 100 if total_checks > 0 else 0

    render_kpi_cards([
        {"label": "Audit Logs Recorded", "value": str(total_checks), "icon": None, "delta": "Total Scans"},
        {"label": "Confirmed Integrity", "value": str(passed), "icon": None,
         "delta": f"+{pass_rate:.0f}% Compliance Rate"},
        {"label": "Policy Violations (WARN)", "value": str(warned), "icon": None,
         "delta": "REHABILITATION RECOMMENDED" if warned > 0 else "ZERO VIOLATIONS"},
        {"label": "System Failures (FAIL)", "value": str(failed), "icon": None,
         "delta": "IMMEDIATE REMEDIATION" if failed > 0 else "ENVIRONMENT SECURE"},
    ])

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Global Governance Score ----
    render_section_header("Aggregate Governance Index", f"Target compliance: 100.0% | Current performance: {pass_rate:.1f}%")

    score_color = "#10B981" if pass_rate >= 90 else "#F59E0B" if pass_rate >= 70 else "#EF4444"
    st.markdown(f"""
    <div style="
        background: {COLORS['surface']};
        border-radius: 16px;
        padding: 30px;
        border: 1px solid {COLORS['border']};
        margin-bottom: 12px;
    ">
        <div style="
            background: rgba(0,0,0,0.1);
            border-radius: 12px;
            height: 48px;
            overflow: hidden;
            position: relative;
        ">
            <div style="
                background: {score_color};
                width: {pass_rate}%;
                height: 100%;
                border-radius: 12px;
                transition: width 1.5s ease-out;
                display: flex;
                align-items: center;
                justify-content: center;
            ">
                <span style="color: white; font-weight: 800; font-size: 18px;">{pass_rate:.1f}%</span>
            </div>
        </div>
        <div style="display: flex; justify-content: space-between; margin-top: 20px; padding: 0 10px; font-weight: 600; font-size: 14px; text-transform: uppercase; color: {COLORS['text_muted']};">
            <span style="color: #10B981;">{passed} PASS</span>
            <span style="color: #F59E0B;">{warned} WARN</span>
            <span style="color: #EF4444;">{failed} FAIL</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Granular Audit Telemetry ----
    render_section_header("Strategic Audit Logs", "Standardized validation protocols breakdown")

    tabs = st.tabs(["ALL RECORDS", "COMPLETENESS", "VALIDITY", "CONSISTENCY", "ANOMALY", "FRESHNESS"])
    types = [None, "completeness", "validity", "consistency", "anomaly", "freshness"]

    for tab, t_id in zip(tabs, types):
        with tab:
            if t_id:
                filtered = df_quality[df_quality["check_type"] == t_id]
            else:
                filtered = df_quality

            if filtered.empty:
                st.info(f"System: No logs detected for {t_id if t_id else 'current profile'}.")
                continue

            # Build standardized telemetry table
            display_rows = []
            for _, row in filtered.iterrows():
                display_rows.append({
                    "STATUS": row["status"],
                    "PROTOCOL": row["check_name"],
                    "TABLE": row["table_name"],
                    "COLUMN": row["column_name"],
                    "METRIC": row["metric_value"],
                    "TARGET": row["threshold"],
                    "DIAGNOSTICS": row["details"],
                })

            display_df = pd.DataFrame(display_rows)
            
            # Semantic highlighting for audit rows
            def color_audit(row):
                if row["STATUS"] == "FAIL":
                    return ['background-color: rgba(239, 68, 68, 0.08); color: #EF4444; font-weight: 500'] * len(row)
                elif row["STATUS"] == "WARN":
                    return ['background-color: rgba(245, 158, 11, 0.08); color: #F59E0B; font-weight: 500'] * len(row)
                else:
                    return [''] * len(row)
            
            st.dataframe(
                display_df.style.apply(color_audit, axis=1),
                use_container_width=True,
                height=450,
            )

    # ---- Warehouse Layer Index ----
    if table_info:
        render_section_header("Warehouse Integrity Inventory", "Multi-layer data volume tracking")

        rows = []
        for table_name, count in sorted(table_info.items()):
            layer = (
                "BRONZE (Raw)" if table_name.startswith("bronze") else
                "SILVER (Cleaned)" if table_name.startswith("silver") else
                "GOLD (Analytics)" if table_name.startswith("gold") else
                "SYSTEM (Core)"
            )
            rows.append({"DOMAIN": layer, "TABLE NAME": table_name, "RECORD COUNT": f"{count:,}"})

        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=350)


if __name__ == "__main__":
    main()
