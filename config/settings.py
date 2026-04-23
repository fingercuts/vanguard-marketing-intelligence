"""
Central configuration for the Vanguard Marketing Intelligence Platform.
All paths, constants, and pipeline settings are managed here.
"""

import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env (if present)
# ---------------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------------
# Project Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DB_DIR = PROJECT_ROOT / "db"
LOG_DIR = PROJECT_ROOT / "logs"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DUCKDB_PATH = str(DB_DIR / os.getenv("DUCKDB_PATH", "vanguard_intelligence.duckdb"))

# ---------------------------------------------------------------------------
# Data Generation
# ---------------------------------------------------------------------------
DATA_START_DATE = os.getenv("DATA_START_DATE", "2024-01-01")
DATA_END_DATE = os.getenv("DATA_END_DATE", datetime.now().strftime("%Y-%m-%d"))

NUM_CAMPAIGNS = int(os.getenv("NUM_CAMPAIGNS", "10"))
NUM_ADGROUPS_PER_CAMPAIGN = 3

CHANNELS = os.getenv("CHANNELS", "Google Ads,Facebook Ads,TikTok Ads").split(",")
CHANNELS = [c.strip() for c in CHANNELS]

CAMPAIGN_OBJECTIVES = ["Awareness", "Traffic", "Conversions", "App Install", "Lead Gen"]
BID_STRATEGIES = ["Manual CPC", "Target CPA", "Maximize Conversions", "Target ROAS"]
TARGETING_TYPES = ["Interest", "Lookalike", "Remarketing", "Broad", "Custom Audience"]

# Channel-specific performance profiles (mean multipliers)
CHANNEL_PROFILES = {
    "Google Ads": {
        "impressions_base": 3500,
        "ctr_range": (0.03, 0.08),
        "cpc_range": (0.30, 1.20),
        "conv_rate_range": (0.05, 0.15),
        "revenue_per_conv": (25, 80),
    },
    "Facebook Ads": {
        "impressions_base": 4000,
        "ctr_range": (0.02, 0.06),
        "cpc_range": (0.15, 0.90),
        "conv_rate_range": (0.04, 0.12),
        "revenue_per_conv": (20, 70),
    },
    "TikTok Ads": {
        "impressions_base": 5000,
        "ctr_range": (0.04, 0.12),
        "cpc_range": (0.10, 0.60),
        "conv_rate_range": (0.06, 0.18),
        "revenue_per_conv": (30, 100),
    },
}

# Hourly traffic distribution weights (24 hours, index 0 = midnight)
HOURLY_WEIGHTS = [
    0.2, 0.15, 0.10, 0.08, 0.07, 0.08,   # 00-05 (low)
    0.15, 0.30, 0.50, 0.65, 0.75, 0.80,   # 06-11 (ramp up)
    0.85, 0.80, 0.75, 0.70, 0.75, 0.85,   # 12-17 (afternoon)
    0.95, 1.00, 0.90, 0.70, 0.50, 0.35,   # 18-23 (evening peak -> decline)
]

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "full")  # full | incremental
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
BATCH_ID_FORMAT = "%Y%m%d_%H%M%S"

# ---------------------------------------------------------------------------
# Data Quality Thresholds
# ---------------------------------------------------------------------------
QUALITY_THRESHOLDS = {
    "max_null_pct": 0.01,          # Max 1% nulls per column
    "spend_min": 0.0,              # Spend must be >= 0
    "ctr_max": 1.0,                # CTR must be <= 1
    "roas_max": 50.0,              # ROAS outlier threshold
    "zscore_threshold": 3.0,       # Anomaly detection Z-score
    "min_daily_rows": 100,         # Minimum rows per day
    "max_staleness_hours": 48,     # Max age of latest data
}

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8501"))
DASHBOARD_THEME = os.getenv("DASHBOARD_THEME", "light")

# Color palette for the dashboard (Enterprise Premium)
COLORS = {
    "primary": "#0F172A",       # Slate 900 (Navy)
    "secondary": "#800000",     # Maroon
    "success": "#065F46",       # Emerald 800
    "warning": "#92400E",       # Amber 800
    "danger": "#991B1B",        # Red 800
    "info": "#0E7490",          # Cyan 700
    "background": "#FFFFFF",    # Pure white
    "surface": "#F8FAFC",       # Slate 50
    "surface_light": "#F1F5F9", # Slate 100
    "border": "#E2E8F0",        # Slate 200
    "text": "#0F172A",          # Slate 900
    "text_muted": "#475569",    # Slate 600
    "google": "#174EA6",      # Enterprise Google Blue
    "facebook": "#0668E1",    # Enterprise Facebook Blue
    "tiktok": "#000000",      # TikTok Black
}

PLOTLY_TEMPLATE = "plotly_white"
