"""
Sidebar filter components for the Streamlit dashboard.
Provides date range, channel, and campaign dimensions.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta


def render_sidebar_filters(df: pd.DataFrame, show_campaign: bool = True) -> dict:
    """
    Render sidebar governance filters.
    """
    st.sidebar.markdown(f"""
    <div style="
        padding: 10px 0 30px 0;
        margin-bottom: 20px;
        border-bottom: 1px solid rgba(255,255,255,0.05);
    ">
        <h1 style="
            font-size: 14px;
            font-weight: 800;
            color: white;
            margin: 0;
            letter-spacing: 0.1em;
            text-transform: uppercase;
        ">Vanguard Intelligence</h1>
        <p style="color: rgba(255,255,255,0.4); font-size: 10px; margin: 4px 0 0 0; text-transform: uppercase; letter-spacing: 0.05em;">Analytical Control Plane</p>
    </div>
    """, unsafe_allow_html=True)

    filters = {}

    # Ensure date column is datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        min_date = df["date"].min().date()
        max_date = df["date"].max().date()

        st.sidebar.markdown('<p style="font-size: 11px; font-weight: 700; color: rgba(255,255,255,0.6); text-transform: uppercase;">Temporal Scope</p>', unsafe_allow_html=True)
        date_range = st.sidebar.date_input(
            "Select Range",
            value=(max_date - timedelta(days=30), max_date),
            min_value=min_date,
            max_value=max_date,
            key="date_filter",
            label_visibility="collapsed"
        )
        filters["date_range"] = date_range

    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    # Channel filter
    if "channel" in df.columns:
        channels = sorted(df["channel"].unique().tolist())
        st.sidebar.markdown('<p style="font-size: 11px; font-weight: 700; color: rgba(255,255,255,0.6); text-transform: uppercase;">Network Context</p>', unsafe_allow_html=True)
        selected_channels = st.sidebar.multiselect(
            "Select Networks",
            options=channels,
            default=channels,
            key="channel_filter",
            label_visibility="collapsed"
        )
        filters["channels"] = selected_channels

    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    # Campaign filter
    if show_campaign and "campaign_id" in df.columns:
        campaigns = sorted(df["campaign_id"].unique().tolist())
        st.sidebar.markdown('<p style="font-size: 11px; font-weight: 700; color: rgba(255,255,255,0.6); text-transform: uppercase;">Campaign Protocol</p>', unsafe_allow_html=True)
        selected_campaigns = st.sidebar.multiselect(
            "Select Campaigns",
            options=campaigns,
            default=campaigns,
            key="campaign_filter",
            label_visibility="collapsed"
        )
        filters["campaigns"] = selected_campaigns

    st.sidebar.markdown("<div style='flex-grow: 1;'></div>", unsafe_allow_html=True)
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        '<p style="color: rgba(255,255,255,0.3); font-size: 10px; text-align: center; text-transform: uppercase; letter-spacing: 0.1em;">'
        'Enterprise Intelligence Engine<br>Vanguard v1.1.0</p>',
        unsafe_allow_html=True,
    )

    return filters


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """Apply governance filters to the telemetry stream."""
    filtered = df.copy()
    filtered["date"] = pd.to_datetime(filtered["date"])

    # Date range
    if "date_range" in filters and len(filters["date_range"]) == 2:
        start, end = filters["date_range"]
        filtered = filtered[
            (filtered["date"].dt.date >= start) &
            (filtered["date"].dt.date <= end)
        ]

    # Channels
    if "channels" in filters and "channel" in filtered.columns:
        filtered = filtered[filtered["channel"].isin(filters["channels"])]

    # Campaigns
    if "campaigns" in filters and "campaign_id" in filtered.columns:
        filtered = filtered[filtered["campaign_id"].isin(filters["campaigns"])]

    return filtered
