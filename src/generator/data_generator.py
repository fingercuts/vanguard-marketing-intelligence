"""
Multi-channel marketing data simulator (Vectorized).

Generates synthetic advertising performance data, user behavior events,
and dimension tables spanning from 2024-01-01 to the current date.

Implementation Note: Uses NumPy vectorized operations for high-velocity 
generation, mimicking enterprise-level data volume.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import (
    DATA_DIR,
    DATA_START_DATE,
    DATA_END_DATE,
    NUM_CAMPAIGNS,
    NUM_ADGROUPS_PER_CAMPAIGN,
    CHANNELS,
    CHANNEL_PROFILES,
    HOURLY_WEIGHTS,
    CAMPAIGN_OBJECTIVES,
    BID_STRATEGIES,
    TARGETING_TYPES,
)
from src.utils.logging_config import setup_logging

logger = setup_logging("data_generator")

# Locked seed to ensure reproducible baseline for budget optimization testing
np.random.seed(42)


# ===================================================================
# DIMENSION TABLES
# ===================================================================

def generate_campaigns() -> pd.DataFrame:
    """Generate the campaign dimension table."""
    logger.info(f"Generating {NUM_CAMPAIGNS} operational campaigns...")

    start = datetime.strptime(DATA_START_DATE, "%Y-%m-%d")
    end = datetime.strptime(DATA_END_DATE, "%Y-%m-%d")
    total_days = (end - start).days

    rows = []
    for i in range(NUM_CAMPAIGNS):
        offset = int(total_days * (i / NUM_CAMPAIGNS) * 0.3)
        camp_start = start + timedelta(days=offset)
        if np.random.random() < 0.3:
            camp_end = camp_start + timedelta(days=np.random.randint(30, 180))
            status = "Completed" if camp_end < end else "Active"
        else:
            camp_end = None
            status = "Active"

        rows.append({
            "campaign_id": f"CMP{i:03d}",
            "campaign_name": f"Campaign_{CAMPAIGN_OBJECTIVES[i % len(CAMPAIGN_OBJECTIVES)]}_{i:03d}",
            "objective": CAMPAIGN_OBJECTIVES[i % len(CAMPAIGN_OBJECTIVES)],
            "budget_daily": round(np.random.uniform(50, 500), 2),
            "start_date": camp_start.strftime("%Y-%m-%d"),
            "end_date": camp_end.strftime("%Y-%m-%d") if camp_end else None,
            "status": status,
        })

    df = pd.DataFrame(rows)
    path = DATA_DIR / "campaigns.parquet"
    df.to_parquet(path, compression="snappy")
    logger.info(f"  [EXPORT] Saved {len(df)} campaigns to {path}")
    return df


def generate_adgroups(campaigns_df: pd.DataFrame) -> pd.DataFrame:
    """Generate the ad group dimension table."""
    logger.info("Generating ad group mapping...")

    rows = []
    for _, camp in campaigns_df.iterrows():
        for j in range(NUM_ADGROUPS_PER_CAMPAIGN):
            rows.append({
                "adgroup_id": f"{camp['campaign_id']}_AG{j:02d}",
                "campaign_id": camp["campaign_id"],
                "adgroup_name": f"{camp['campaign_name']}_AdGroup_{j:02d}",
                "targeting_type": np.random.choice(TARGETING_TYPES),
                "bid_strategy": np.random.choice(BID_STRATEGIES),
            })

    df = pd.DataFrame(rows)
    path = DATA_DIR / "adgroups.parquet"
    df.to_parquet(path, compression="snappy")
    logger.info(f"  [EXPORT] Saved {len(df)} ad groups to {path}")
    return df


# ===================================================================
# SEASONAL / TIME HELPERS (Calibrated to 2023-2024 Retail Indexes)
# ===================================================================

SEASONAL_MAP = {
    1: 0.85, 2: 0.80, 3: 0.90, 4: 0.95, 5: 1.00, 6: 1.05,
    7: 1.00, 8: 0.95, 9: 1.05, 10: 1.10, 11: 1.25, 12: 1.40,
}

DOW_MAP = {0: 1.0, 1: 1.02, 2: 1.05, 3: 1.03, 4: 0.95, 5: 0.85, 6: 0.80}


# ===================================================================
# FACT: AD PERFORMANCE (Vectorized)
# ===================================================================

def generate_ads_performance(campaigns_df: pd.DataFrame, adgroups_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate ad performance data via vectorized operations.
    
    Calibration logic:
    - Incorporates seasonal bid pressure (Q4 surge)
    - Simulates platform-specific CTR/CPA variances
    - Injects specific data quality failures (negatives, outliers) for 
      governance testing.
    """
    logger.info("Simulating ad performance (vectorized pipeline)...")

    start = datetime.strptime(DATA_START_DATE, "%Y-%m-%d")
    end = datetime.strptime(DATA_END_DATE, "%Y-%m-%d")

    dates = pd.date_range(start, end, freq="D")
    logger.info(f"  Range: {start.date()} to {end.date()} ({len(dates)} days)")

    camp_ranges = {}
    for _, c in campaigns_df.iterrows():
        c_start = datetime.strptime(c["start_date"], "%Y-%m-%d")
        c_end = datetime.strptime(c["end_date"], "%Y-%m-%d") if pd.notna(c["end_date"]) else end
        camp_ranges[c["campaign_id"]] = (c_start, c_end)

    camp_adgroups = adgroups_df.groupby("campaign_id")["adgroup_id"].apply(list).to_dict()

    all_rows = []

    for channel in CHANNELS:
        profile = CHANNEL_PROFILES[channel]
        logger.info(f"  Processing profile: {channel}...")

        for _, camp in campaigns_df.iterrows():
            cid = camp["campaign_id"]
            c_start, c_end = camp_ranges[cid]
            agids = camp_adgroups.get(cid, [f"{cid}_AG00"])

            # Filter dates to campaign active period
            mask = (dates >= c_start) & (dates <= c_end)
            active_dates = dates[mask]

            if len(active_dates) == 0:
                continue

            n = len(active_dates)

            # Seasonal + DOW factors
            months = active_dates.month
            dows = active_dates.dayofweek
            seasonal = np.array([SEASONAL_MAP[m] for m in months])
            dow_factor = np.array([DOW_MAP[d] for d in dows])
            combined = seasonal * dow_factor

            # Impressions: log-normal
            # Mimicking standard display/search traffic variance
            base_imp = profile["impressions_base"]
            impressions = np.random.lognormal(
                mean=np.log(base_imp * combined), sigma=0.3
            ).astype(int)
            impressions = np.maximum(impressions, 10)

            # Clicks
            ctr_vals = np.random.uniform(*profile["ctr_range"], size=n) * combined
            ctr_vals = np.minimum(ctr_vals, 0.95)
            clicks = (impressions * ctr_vals).astype(int)

            # Spend
            cpc_vals = np.random.uniform(*profile["cpc_range"], size=n)
            spend = np.round(clicks * cpc_vals, 2)

            # Conversions
            conv_rate = np.random.uniform(*profile["conv_rate_range"], size=n)
            conversions = (clicks * conv_rate).astype(int)

            # Revenue
            rev_per_conv = np.random.uniform(*profile["revenue_per_conv"], size=n)
            revenue = np.round(conversions * rev_per_conv, 2)

            # --- DATA QUALITY IMPERFECTION INJECTION ---
            # Simulate upstream API billing errors (negative spend)
            neg_mask = np.random.random(size=n) < 0.005 # 0.5% frequency
            spend[neg_mask] = -np.abs(spend[neg_mask]) * 2

            # Simulate bot activity/flood (Anomaly/Outliers)
            outlier_mask = np.random.random(size=n) < 0.002 # 0.2% frequency
            spend[outlier_mask] = spend[outlier_mask] * 25
            clicks[outlier_mask] = clicks[outlier_mask] * 15

            # Simulate referential integrity loss (Orphaned Adgroups)
            agid_sample = np.random.choice(agids, size=n)
            orphan_mask = np.random.random(size=n) < 0.003 # 0.3% frequency
            agid_final = np.where(orphan_mask, "CMP999_AG99", agid_sample)

            # Hour sampling
            hours = np.random.choice(24, size=n, p=np.array(HOURLY_WEIGHTS) / sum(HOURLY_WEIGHTS))

            chunk = pd.DataFrame({
                "date": active_dates.strftime("%Y-%m-%d"),
                "hour": hours,
                "campaign_id": cid,
                "adgroup_id": agid_final,
                "channel": channel,
                "impressions": impressions,
                "clicks": clicks,
                "spend": spend,
                "conversions": conversions,
                "revenue": revenue,
            })
            all_rows.append(chunk)

    df = pd.concat(all_rows, ignore_index=True)
    path = DATA_DIR / "ads_performance.parquet"
    df.to_parquet(path, compression="snappy")
    logger.info(f"  [SUCCESS] Saved {len(df):,} ad performance rows to {path}")
    return df


# ===================================================================
# FACT: DIGITAL EVENTS (Vectorized)
# ===================================================================

EVENT_TYPES = [
    "page_view", "page_view", "page_view",
    "session_start", "scroll", "click",
    "add_to_cart", "begin_checkout", "purchase",
    "video_start", "video_complete", "file_download",
    "sign_up", "login",
]

PAGES = [
    "/", "/products", "/products/detail", "/pricing",
    "/about", "/contact", "/blog", "/blog/post",
    "/cart", "/checkout", "/thank-you",
]

SOURCES = ["google", "facebook", "tiktok", "direct", "organic", "email", "referral"]
MEDIUMS = ["cpc", "cpm", "social", "organic", "email", "referral", "(none)"]


def generate_ga_events(campaigns_df: pd.DataFrame) -> pd.DataFrame:
    """Generate session-level interaction data."""
    logger.info("Simulating user behavior events (vectorized)...")

    start = datetime.strptime(DATA_START_DATE, "%Y-%m-%d")
    end = datetime.strptime(DATA_END_DATE, "%Y-%m-%d")
    dates = pd.date_range(start, end, freq="D")
    campaign_ids = campaigns_df["campaign_id"].tolist()
    num_days = len(dates)

    # Pre-compute daily sessions count
    seasonal_factors = np.array([SEASONAL_MAP[d.month] for d in dates])
    daily_sessions = (np.random.randint(200, 400, size=num_days) * seasonal_factors).astype(int)
    total_sessions = int(daily_sessions.sum())
    
    # Events per session (calibration: avg of 4 interactions)
    events_per_session = np.random.randint(1, 9, size=total_sessions)
    total_events = int(events_per_session.sum())

    # Build event-to-session mapping
    session_day_idx = np.repeat(np.arange(num_days), daily_sessions)
    event_day_idx = np.repeat(session_day_idx, events_per_session)

    date_strings = dates.strftime("%Y-%m-%d").values
    event_dates = date_strings[event_day_idx]

    session_ints = np.arange(total_sessions)
    event_session_ids = np.repeat(session_ints, events_per_session)
    user_ints = np.random.randint(1, 50000, size=total_sessions)
    event_user_ids = np.repeat(user_ints, events_per_session)

    hourly_probs = np.array(HOURLY_WEIGHTS) / sum(HOURLY_WEIGHTS)
    session_hours = np.random.choice(24, size=total_sessions, p=hourly_probs)
    event_hours = np.repeat(session_hours, events_per_session)

    session_sources = np.random.choice(SOURCES, size=total_sessions)
    event_sources = np.repeat(session_sources, events_per_session)

    session_mediums = np.random.choice(MEDIUMS, size=total_sessions)
    event_mediums = np.repeat(session_mediums, events_per_session)

    event_names = np.random.choice(EVENT_TYPES, size=total_events)
    event_pages = np.random.choice(PAGES, size=total_events)

    # Attribution for paid traffic
    paid_mask = np.isin(event_sources, ["google", "facebook", "tiktok"])
    camp_ids = np.where(
        paid_mask,
        np.random.choice(campaign_ids, size=total_events),
        None
    )

    # Simulate attribution loss (Missing UTM parameters on paid traffic)
    missing_camp_mask = (np.random.random(size=total_events) < 0.05) & paid_mask
    camp_ids[missing_camp_mask] = None

    # String encoding for IDs
    session_id_strs = np.array([f"S{x:08d}" for x in range(total_sessions)])
    event_session_strs = session_id_strs[event_session_ids]
    
    user_id_strs_unique = np.array([f"U{x:06d}" for x in range(50000)])
    event_user_strs = user_id_strs_unique[event_user_ids]

    df = pd.DataFrame({
        "date": event_dates,
        "hour": event_hours.astype(int),
        "event_name": event_names,
        "session_id": event_session_strs,
        "user_id": event_user_strs,
        "page": event_pages,
        "source": event_sources,
        "medium": event_mediums,
        "campaign_id": camp_ids,
    })

    path = DATA_DIR / "ga_events.parquet"
    df.to_parquet(path, compression="snappy")
    logger.info(f"  [SUCCESS] Saved {len(df):,} behavior event rows to {path}")
    return df


# ===================================================================
# MAIN ORCHESTRATION
# ===================================================================

def generate_all_data():
    """Execute the full simulation suite."""
    logger.info("-" * 60)
    logger.info("VANGUARD TRAFFIC SIMULATION ENGINE")
    logger.info(f"Configuration: {DATA_START_DATE} -> {DATA_END_DATE}")
    logger.info("-" * 60)

    start_time = datetime.now()

    campaigns_df = generate_campaigns()
    adgroups_df = generate_adgroups(campaigns_df)
    ads_df = generate_ads_performance(campaigns_df, adgroups_df)
    ga_df = generate_ga_events(campaigns_df)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("-" * 60)
    logger.info("SIMULATION COMPLETE")
    logger.info(f"  Total Duration: {elapsed:.1f}s")
    logger.info("-" * 60)

    return {
        "campaigns": campaigns_df,
        "adgroups": adgroups_df,
        "ads_performance": ads_df,
        "ga_events": ga_df,
    }


if __name__ == "__main__":
    generate_all_data()
