"""
Reusable chart helper functions for the Streamlit dashboard.
Provides pre-configured Plotly chart builders with consistent enterprise styling.
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import COLORS, PLOTLY_TEMPLATE


CHANNEL_COLORS = {
    "Google Ads": COLORS["google"],
    "Facebook Ads": COLORS["facebook"],
    "TikTok Ads": COLORS["tiktok"],
}


def _apply_themed_layout(fig):
    """Apply consistent enterprise layout to any Plotly figure."""
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Outfit, sans-serif", color=COLORS["text"]),
        legend=dict(
            bgcolor="rgba(255,255,255,0.05)",
            bordercolor=COLORS["border"],
            borderwidth=1,
            font=dict(size=11, color=COLORS["text"]),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=40, r=20, t=60, b=40),
        hoverlabel=dict(
            bgcolor=COLORS["surface"],
            font_size=13,
            font_family="Outfit, sans-serif",
            bordercolor=COLORS["border"],
        ),
    )
    fig.update_xaxes(
        gridcolor=COLORS["border"],
        zeroline=False,
        tickfont=dict(color=COLORS["text_muted"], size=10)
    )
    fig.update_yaxes(
        gridcolor=COLORS["border"],
        zeroline=False,
        tickfont=dict(color=COLORS["text_muted"], size=10)
    )
    return fig


def line_chart(df: pd.DataFrame, x: str, y: str, color: str = None,
               title: str = "", y_format: str = None):
    """Create a styled enterprise line chart."""
    fig = px.line(
        df, x=x, y=y, color=color,
        title=title,
        color_discrete_map=CHANNEL_COLORS if color else None,
    )
    fig.update_traces(line=dict(width=2.5))
    if y_format:
        fig.update_yaxes(tickformat=y_format)
    return _apply_themed_layout(fig)


def bar_chart(df: pd.DataFrame, x: str, y: str, color: str = None,
              title: str = "", orientation: str = "v", y_format: str = None):
    """Create a styled enterprise bar chart."""
    fig = px.bar(
        df, x=x, y=y, color=color,
        title=title,
        barmode="group",
        color_discrete_map=CHANNEL_COLORS if color else None,
        orientation=orientation,
    )
    fig.update_traces(marker_line_width=0, opacity=0.9)
    if y_format:
        fig.update_yaxes(tickformat=y_format)
    return _apply_themed_layout(fig)


def area_chart(df: pd.DataFrame, x: str, y: str, color: str = None,
               title: str = ""):
    """Create a styled enterprise area chart."""
    fig = px.area(
        df, x=x, y=y, color=color,
        title=title,
        color_discrete_map=CHANNEL_COLORS if color else None,
    )
    fig.update_traces(line=dict(width=2))
    return _apply_themed_layout(fig)


def pie_chart(df: pd.DataFrame, values: str, names: str, title: str = ""):
    """Create a styled enterprise donut chart."""
    fig = px.pie(
        df, values=values, names=names, color=names,
        title=title,
        color_discrete_map=CHANNEL_COLORS,
        hole=0.6,
    )
    fig.update_traces(
        textposition="outside",
        textinfo="percent",
        textfont_size=11,
        marker=dict(line=dict(color="#FFFFFF", width=1))
    )
    return _apply_themed_layout(fig)


def heatmap_chart(df: pd.DataFrame, x: str, y: str, z: str, title: str = ""):
    """Create a styled heatmap with strategic colorscale."""
    pivot = df.pivot_table(values=z, index=y, columns=x, aggfunc="mean")
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale="Blues",
        hoverongaps=False,
    ))
    fig.update_layout(title=title)
    return _apply_themed_layout(fig)


def funnel_chart(labels: list, values: list, title: str = ""):
    """Create a premium conversion funnel chart with architectural shading."""
    # Architectural color palette
    colors = [COLORS["primary"], COLORS["secondary"], COLORS["success"]]
    
    fig = go.Figure(go.Funnel(
        y=labels,
        x=values,
        textposition="inside",
        textinfo="label+value+percent initial",
        opacity=0.9,
        marker=dict(
            color=colors,
            line=dict(width=0)
        ),
        connector=dict(
            fillcolor="rgba(0, 0, 0, 0.05)",
            line=dict(width=0)
        ),
        hoverinfo="x+percent previous"
    ))
    fig.update_layout(
        title=title,
        margin=dict(t=50, l=10, r=10, b=10),
    )
    return _apply_themed_layout(fig)


def dual_axis_chart(df: pd.DataFrame, x: str, y1: str, y2: str,
                     y1_name: str = "", y2_name: str = "",
                     title: str = ""):
    """Create an enterprise dual Y-axis chart."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            x=df[x], y=df[y1],
            name=y1_name or y1,
            marker_color=COLORS["primary"],
            opacity=0.75,
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=df[x], y=df[y2],
            name=y2_name or y2,
            line=dict(color=COLORS["success"], width=2.5),
            mode="lines",
        ),
        secondary_y=True,
    )

    fig.update_layout(title=title, barmode="group", hovermode="x unified")
    fig.update_yaxes(title_text=y1_name or y1, secondary_y=False)
    fig.update_yaxes(title_text=y2_name or y2, secondary_y=True)
    return _apply_themed_layout(fig)
