"""Tests for the data generator module."""

import pytest
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.generator.data_generator import (
    generate_campaigns,
    generate_adgroups,
    generate_ads_performance,
    generate_ga_events,
)
from config.settings import NUM_CAMPAIGNS, CHANNELS, NUM_ADGROUPS_PER_CAMPAIGN


class TestCampaignGenerator:
    """Tests for campaign dimension table generation."""

    def test_campaign_count(self):
        df = generate_campaigns()
        assert len(df) == NUM_CAMPAIGNS

    def test_campaign_columns(self):
        df = generate_campaigns()
        expected_cols = {"campaign_id", "campaign_name", "objective",
                         "budget_daily", "start_date", "end_date", "status"}
        assert expected_cols.issubset(set(df.columns))

    def test_campaign_id_format(self):
        df = generate_campaigns()
        assert all(cid.startswith("CMP") for cid in df["campaign_id"])

    def test_campaign_budget_positive(self):
        df = generate_campaigns()
        assert (df["budget_daily"] > 0).all()

    def test_campaign_status_values(self):
        df = generate_campaigns()
        valid_statuses = {"Active", "Completed"}
        assert set(df["status"].unique()).issubset(valid_statuses)


class TestAdgroupGenerator:
    """Tests for ad group dimension table generation."""

    def test_adgroup_count(self):
        campaigns = generate_campaigns()
        adgroups = generate_adgroups(campaigns)
        assert len(adgroups) == NUM_CAMPAIGNS * NUM_ADGROUPS_PER_CAMPAIGN

    def test_adgroup_referential_integrity(self):
        campaigns = generate_campaigns()
        adgroups = generate_adgroups(campaigns)
        campaign_ids = set(campaigns["campaign_id"])
        adgroup_campaign_ids = set(adgroups["campaign_id"])
        assert adgroup_campaign_ids.issubset(campaign_ids)


class TestAdsPerformance:
    """Tests for ad performance fact table generation."""

    @pytest.fixture(scope="class")
    def ads_data(self):
        campaigns = generate_campaigns()
        adgroups = generate_adgroups(campaigns)
        return generate_ads_performance(campaigns, adgroups)

    def test_no_negative_impressions(self, ads_data):
        assert (ads_data["impressions"] >= 0).all()

    def test_no_negative_spend(self, ads_data):
        assert (ads_data["spend"] >= 0).all()

    def test_clicks_lte_impressions(self, ads_data):
        # Clicks should generally not exceed impressions
        violation_pct = (ads_data["clicks"] > ads_data["impressions"]).mean()
        assert violation_pct < 0.01  # Less than 1% violations

    def test_channels_present(self, ads_data):
        for ch in CHANNELS:
            assert ch in ads_data["channel"].unique()

    def test_hour_range(self, ads_data):
        assert ads_data["hour"].min() >= 0
        assert ads_data["hour"].max() <= 23

    def test_has_data(self, ads_data):
        assert len(ads_data) > 10000  # Should have substantial data


class TestGAEvents:
    """Tests for GA events generation."""

    @pytest.fixture(scope="class")
    def ga_data(self):
        campaigns = generate_campaigns()
        return generate_ga_events(campaigns)

    def test_event_types_present(self, ga_data):
        assert "page_view" in ga_data["event_name"].unique()
        assert "purchase" in ga_data["event_name"].unique()

    def test_session_ids_not_null(self, ga_data):
        assert ga_data["session_id"].notna().all()

    def test_user_ids_not_null(self, ga_data):
        assert ga_data["user_id"].notna().all()

    def test_has_data(self, ga_data):
        assert len(ga_data) > 10000
