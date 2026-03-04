"""Tests for the data pipeline and analytics engine."""
import os
import sys
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data_pipeline import (
    _revenue_bucket, _employee_tier, _lead_grade,
    process_companies, process_contacts, process_deals,
    process_marketing_events, process_email_campaigns,
    process_support_tickets, process_web_analytics,
    aggregate_revenue_by_quarter, aggregate_pipeline_stages,
    aggregate_marketing_by_channel, aggregate_email_performance,
    aggregate_support_by_category, aggregate_web_by_page,
)


# ── Helper factories ────────────────────────────────────────────────

def make_companies(n=100):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "company_id": np.arange(1, n + 1),
        "company_name": [f"Co_{i}" for i in range(n)],
        "industry": rng.choice(["Tech", "Finance"], size=n),
        "region": rng.choice(["NA", "EU"], size=n),
        "employee_count": rng.integers(5, 5000, size=n),
        "annual_revenue": rng.lognormal(14, 2, size=n).round(2),
        "founded_year": rng.integers(1980, 2023, size=n),
        "website_traffic_monthly": rng.integers(100, 1_000_000, size=n),
        "created_at": pd.date_range("2023-01-01", periods=n, freq="h"),
    })


def make_contacts(n=200):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "contact_id": np.arange(1, n + 1),
        "email": [f"u{i}@test.com" for i in range(n)],
        "company_id": rng.integers(1, 50, size=n),
        "job_title": rng.choice(["CEO", "Eng"], size=n),
        "lead_source": rng.choice(["Organic", "Paid"], size=n),
        "lead_score": rng.integers(0, 101, size=n),
        "lifecycle_stage": rng.choice(["Lead", "MQL", "Customer"], size=n),
        "region": rng.choice(["NA", "EU"], size=n),
        "first_touch_date": pd.date_range("2023-01-01", periods=n, freq="h"),
        "num_page_views": rng.integers(0, 200, size=n),
        "num_form_submissions": rng.integers(0, 10, size=n),
    })


def make_deals(n=500):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "deal_id": np.arange(1, n + 1),
        "contact_id": rng.integers(1, 100, size=n),
        "company_id": rng.integers(1, 50, size=n),
        "deal_name": [f"Deal_{i}" for i in range(n)],
        "amount": rng.lognormal(9, 1.5, size=n).round(2),
        "stage": rng.choice(["Prospecting", "Qualification", "Closed Won", "Closed Lost"], size=n),
        "pipeline": rng.choice(["Enterprise", "SMB"], size=n),
        "probability": rng.uniform(0, 100, size=n).round(1),
        "days_in_pipeline": rng.integers(1, 300, size=n),
        "created_at": pd.date_range("2023-01-01", periods=n, freq="4h"),
        "industry": rng.choice(["Tech", "Finance"], size=n),
        "region": rng.choice(["NA", "EU"], size=n),
    })


def make_marketing(n=1000):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "event_id": np.arange(1, n + 1),
        "contact_id": rng.integers(1, 100, size=n),
        "event_type": rng.choice(["Page View", "Email Open"], size=n),
        "channel": rng.choice(["Organic", "Paid", "Social"], size=n),
        "campaign_id": rng.integers(1, 100, size=n),
        "utm_source": rng.choice(["google", "facebook"], size=n),
        "device_type": rng.choice(["Desktop", "Mobile"], size=n),
        "session_duration_sec": rng.integers(1, 600, size=n),
        "page_depth": rng.integers(1, 15, size=n),
        "timestamp": pd.date_range("2023-01-01", periods=n, freq="30min"),
    })


def make_emails(n=500):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "email_event_id": np.arange(1, n + 1),
        "contact_id": rng.integers(1, 100, size=n),
        "campaign_id": rng.integers(1, 50, size=n),
        "campaign_type": rng.choice(["Newsletter", "Drip"], size=n),
        "action": rng.choice(["Sent", "Opened", "Clicked", "Bounced"], size=n),
        "subject_line_length": rng.integers(20, 100, size=n),
        "send_hour": rng.integers(0, 24, size=n),
        "send_day_of_week": rng.integers(0, 7, size=n),
        "timestamp": pd.date_range("2023-01-01", periods=n, freq="2h"),
    })


def make_tickets(n=200):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "ticket_id": np.arange(1, n + 1),
        "contact_id": rng.integers(1, 100, size=n),
        "company_id": rng.integers(1, 50, size=n),
        "category": rng.choice(["Bug Report", "Billing", "Feature Request"], size=n),
        "priority": rng.choice(["Critical", "High", "Medium", "Low"], size=n),
        "status": rng.choice(["Open", "Resolved", "Closed"], size=n),
        "resolution_hours": rng.exponential(48, size=n).clip(0.5, 720).round(1),
        "satisfaction_score": rng.choice([1, 2, 3, 4, 5], size=n),
        "num_interactions": rng.integers(1, 20, size=n),
        "created_at": pd.date_range("2023-01-01", periods=n, freq="3h"),
    })


def make_web(n=1000):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "session_id": np.arange(1, n + 1),
        "page_url": rng.choice(["/", "/pricing", "/blog"], size=n),
        "referrer_domain": rng.choice(["google.com", "direct"], size=n),
        "browser": rng.choice(["Chrome", "Safari"], size=n),
        "device_type": rng.choice(["Desktop", "Mobile"], size=n),
        "country": rng.choice(["US", "UK", "DE"], size=n),
        "session_duration_sec": rng.integers(1, 600, size=n),
        "page_views": rng.integers(1, 15, size=n),
        "bounce": rng.choice([0, 1], size=n),
        "conversion": rng.choice([0, 1], size=n, p=[0.95, 0.05]),
        "timestamp": pd.date_range("2023-01-01", periods=n, freq="20min"),
    })


# ── Bucketing tests ─────────────────────────────────────────────────

class TestBucketing:
    def test_revenue_bucket(self):
        s = pd.Series([500, 5000, 30000, 100000, 500000, 2000000])
        result = _revenue_bucket(s)
        assert result.iloc[0] == "< $1K"
        assert result.iloc[-1] == "> $1M"

    def test_employee_tier(self):
        s = pd.Series([3, 30, 100, 500, 3000, 10000])
        result = _employee_tier(s)
        assert result.iloc[0] == "Micro"
        assert result.iloc[-1] == "Mega"

    def test_lead_grade(self):
        s = pd.Series([10, 30, 50, 70, 90])
        result = _lead_grade(s)
        assert result.iloc[0] == "F"
        assert result.iloc[-1] == "A"


# ── Transformation tests ────────────────────────────────────────────

class TestTransformations:
    def test_process_companies(self):
        df = process_companies(make_companies())
        assert "employee_tier" in df.columns
        assert "revenue_per_employee" in df.columns
        assert "company_age" in df.columns
        assert (df["company_age"] >= 0).all()

    def test_process_contacts(self):
        df = process_contacts(make_contacts())
        assert "lead_grade" in df.columns
        assert "engagement_index" in df.columns
        assert (df["engagement_index"] <= 1000).all()

    def test_process_deals(self):
        df = process_deals(make_deals())
        assert "revenue_bucket" in df.columns
        assert "weighted_amount" in df.columns
        assert "velocity_score" in df.columns
        assert "is_won" in df.columns
        assert "quarter" in df.columns
        assert df["is_won"].isin([0, 1]).all()

    def test_process_marketing(self):
        df = process_marketing_events(make_marketing())
        assert "hour" in df.columns
        assert "day_of_week" in df.columns
        assert "is_engaged" in df.columns
        assert "month" in df.columns

    def test_process_emails(self):
        df = process_email_campaigns(make_emails())
        assert "is_opened" in df.columns
        assert "is_clicked" in df.columns
        assert "is_bounced" in df.columns
        assert "is_unsub" in df.columns

    def test_process_tickets(self):
        df = process_support_tickets(make_tickets())
        assert "sla_met" in df.columns
        assert "quarter" in df.columns

    def test_process_web(self):
        df = process_web_analytics(make_web())
        assert "hour" in df.columns
        assert "engaged_session" in df.columns
        assert "month" in df.columns


# ── Aggregation tests ──────────────────────────────────────────────

class TestAggregations:
    def test_revenue_by_quarter(self):
        deals = process_deals(make_deals())
        result = aggregate_revenue_by_quarter(deals)
        assert "total_revenue" in result.columns
        assert "win_count" in result.columns
        assert len(result) > 0

    def test_pipeline_stages(self):
        deals = process_deals(make_deals())
        result = aggregate_pipeline_stages(deals)
        assert "count" in result.columns
        assert "total_value" in result.columns

    def test_marketing_by_channel(self):
        mktg = process_marketing_events(make_marketing())
        result = aggregate_marketing_by_channel(mktg)
        assert "event_count" in result.columns
        assert "engagement_rate" in result.columns

    def test_email_performance(self):
        emails = process_email_campaigns(make_emails())
        result = aggregate_email_performance(emails)
        assert "open_rate" in result.columns
        assert "click_rate" in result.columns

    def test_support_by_category(self):
        tickets = process_support_tickets(make_tickets())
        result = aggregate_support_by_category(tickets)
        assert "sla_compliance" in result.columns
        assert "avg_satisfaction" in result.columns

    def test_web_by_page(self):
        web = process_web_analytics(make_web())
        result = aggregate_web_by_page(web)
        assert "bounce_rate" in result.columns
        assert "conversion_rate" in result.columns


# ── Data integrity tests ──────────────────────────────────────────

class TestDataIntegrity:
    def test_no_null_ids(self):
        for factory in [make_companies, make_contacts, make_deals]:
            df = factory()
            id_col = [c for c in df.columns if c.endswith("_id")][0]
            assert df[id_col].notna().all()

    def test_deals_weighted_amount_positive(self):
        df = process_deals(make_deals())
        assert (df["weighted_amount"] >= 0).all()

    def test_sla_binary(self):
        df = process_support_tickets(make_tickets())
        assert df["sla_met"].isin([0, 1]).all()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
