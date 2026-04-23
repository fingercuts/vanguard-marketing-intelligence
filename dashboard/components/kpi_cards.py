"""
Reusable KPI card components for the Streamlit dashboard.
Renders metric cards with consistent enterprise styling.
"""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import COLORS


def render_kpi_cards(metrics: list):
    """
    Render a row of premium KPI metric cards.
    """
    st.markdown(f"""
    <style>
    .kpi-card {{
        background: {COLORS['surface']};
        border: 1px solid {COLORS['border']};
        border-radius: 12px;
        padding: 24px;
        text-align: left;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.02);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }}
    .kpi-card::before {{
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 4px;
        height: 100%;
        background: {COLORS['primary']};
        opacity: 0;
        transition: opacity 0.3s ease;
    }}
    .kpi-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.05);
        border-color: {COLORS['primary']}40;
    }}
    .kpi-card:hover::before {{
        opacity: 1;
    }}
    </style>
    """, unsafe_allow_html=True)

    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        with col:
            delta_val = m.get('delta', '')
            is_positive = delta_val.startswith('+') or 'ROAS' in delta_val or 'MET' in delta_val
            is_negative = delta_val.startswith('-') or 'SUB-OPTIMAL' in delta_val or 'FAIL' in delta_val
            
            delta_color = "#10B981" if is_positive else "#EF4444" if is_negative else COLORS['text_muted']
            delta_bg = "rgba(16, 185, 129, 0.1)" if is_positive else "rgba(239, 68, 68, 0.1)" if is_negative else "rgba(0, 0, 0, 0.05)"

            st.markdown(f"""
            <div class="kpi-card">
                <div style="
                    font-size: 11px;
                    color: {COLORS['text_muted']};
                    text-transform: uppercase;
                    letter-spacing: 0.1em;
                    font-weight: 600;
                    margin-bottom: 8px;
                ">{m['label']}</div>
                <div style="
                    font-size: 28px;
                    font-weight: 800;
                    color: {COLORS['text']};
                    line-height: 1;
                    margin-bottom: 12px;
                    letter-spacing: -0.5px;
                ">{m['value']}</div>
                <div style="
                    font-size: 11px;
                    color: {delta_color};
                    background: {delta_bg};
                    padding: 4px 10px;
                    border-radius: 4px;
                    display: inline-block;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                ">{delta_val}</div>
            </div>
            """, unsafe_allow_html=True)


def render_section_header(title: str, subtitle: str = ""):
    """Render a styled section header for enterprise dashboards."""
    st.markdown(f"""
    <div style="margin: 40px 0 20px 0;">
        <h2 style="
            color: {COLORS['text']};
            font-size: 18px;
            font-weight: 800;
            margin-bottom: 4px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            display: flex;
            align-items: center;
        ">
            <span style="width: 12px; height: 12px; background: {COLORS['primary']}; display: inline-block; margin-right: 12px; border-radius: 1px;"></span>
            {title}
        </h2>
        <p style="
            color: {COLORS['text_muted']};
            font-size: 14px;
            margin: 0;
            padding-left: 24px;
            font-weight: 400;
        ">{subtitle}</p>
    </div>
    """, unsafe_allow_html=True)


def render_status_badge(status: str):
    """Return an HTML status badge for tabular data."""
    colors = {
        "PASS": ("#10B981", "rgba(16, 185, 129, 0.1)"),
        "FAIL": ("#EF4444", "rgba(239, 68, 68, 0.1)"),
        "WARN": ("#F59E0B", "rgba(245, 158, 11, 0.1)"),
    }
    fg, bg = colors.get(status, ("#64748B", "rgba(100, 116, 139, 0.1)"))
    return f"""
    <span style="
        background: {bg};
        color: {fg};
        padding: 4px 12px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    ">{status}</span>
    """
