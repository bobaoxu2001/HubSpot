#!/usr/bin/env python3
"""
ETL Pipeline for HubSpot Big Data Analytics Platform.

Implements a MapReduce-style processing framework that:
  1. Reads raw Parquet files in configurable chunks
  2. Applies transformations (cleaning, enrichment, feature engineering)
  3. Produces aggregated datasets consumed by the analytics layer
"""
import os
import sys
import time
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.config import (
    RAW_DIR, PROCESSED_DIR, AGGREGATED_DIR,
    CHUNK_SIZE, NUM_PARTITIONS,
)


# ── Transformation helpers ──────────────────────────────────────────

def _revenue_bucket(amount: pd.Series) -> pd.Series:
    bins = [0, 1_000, 10_000, 50_000, 200_000, 1_000_000, float("inf")]
    labels = ["< $1K", "$1K–10K", "$10K–50K", "$50K–200K", "$200K–1M", "> $1M"]
    return pd.cut(amount, bins=bins, labels=labels)


def _employee_tier(count: pd.Series) -> pd.Series:
    bins = [0, 10, 50, 200, 1000, 5000, float("inf")]
    labels = ["Micro", "Small", "Medium", "Large", "Enterprise", "Mega"]
    return pd.cut(count, bins=bins, labels=labels)


def _lead_grade(score: pd.Series) -> pd.Series:
    bins = [-1, 20, 40, 60, 80, 100]
    labels = ["F", "D", "C", "B", "A"]
    return pd.cut(score, bins=bins, labels=labels)


# ── Phase 1: Clean & Enrich ─────────────────────────────────────────

def process_companies(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["employee_tier"] = _employee_tier(df["employee_count"])
    df["revenue_per_employee"] = (df["annual_revenue"] / df["employee_count"].clip(lower=1)).round(2)
    df["company_age"] = 2025 - df["founded_year"]
    return df


def process_contacts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["lead_grade"] = _lead_grade(df["lead_score"])
    df["engagement_index"] = (
        df["num_page_views"] * 1 + df["num_form_submissions"] * 10
    ).clip(upper=1000)
    return df


def process_deals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["revenue_bucket"] = _revenue_bucket(df["amount"])
    df["weighted_amount"] = (df["amount"] * df["probability"] / 100).round(2)
    df["velocity_score"] = (df["probability"] / df["days_in_pipeline"].clip(lower=1)).round(4)
    df["is_won"] = (df["stage"] == "Closed Won").astype(int)
    df["is_lost"] = (df["stage"] == "Closed Lost").astype(int)
    df["quarter"] = pd.to_datetime(df["created_at"]).dt.to_period("Q").astype(str)
    df["month"] = pd.to_datetime(df["created_at"]).dt.to_period("M").astype(str)
    return df


def process_marketing_events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["hour"] = pd.to_datetime(df["timestamp"]).dt.hour
    df["day_of_week"] = pd.to_datetime(df["timestamp"]).dt.day_name()
    df["is_engaged"] = (df["session_duration_sec"] > 60).astype(int)
    df["month"] = pd.to_datetime(df["timestamp"]).dt.to_period("M").astype(str)
    return df


def process_email_campaigns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["is_opened"] = (df["action"].isin(["Opened", "Clicked"])).astype(int)
    df["is_clicked"] = (df["action"] == "Clicked").astype(int)
    df["is_bounced"] = (df["action"] == "Bounced").astype(int)
    df["is_unsub"] = (df["action"].isin(["Unsubscribed", "Spam"])).astype(int)
    df["month"] = pd.to_datetime(df["timestamp"]).dt.to_period("M").astype(str)
    return df


def process_support_tickets(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sla_met"] = (
        ((df["priority"] == "Critical") & (df["resolution_hours"] <= 4)) |
        ((df["priority"] == "High") & (df["resolution_hours"] <= 24)) |
        ((df["priority"] == "Medium") & (df["resolution_hours"] <= 72)) |
        ((df["priority"] == "Low") & (df["resolution_hours"] <= 168))
    ).astype(int)
    df["quarter"] = pd.to_datetime(df["created_at"]).dt.to_period("Q").astype(str)
    return df


def process_web_analytics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["hour"] = pd.to_datetime(df["timestamp"]).dt.hour
    df["day_of_week"] = pd.to_datetime(df["timestamp"]).dt.day_name()
    df["month"] = pd.to_datetime(df["timestamp"]).dt.to_period("M").astype(str)
    df["engaged_session"] = ((df["session_duration_sec"] > 30) & (df["bounce"] == 0)).astype(int)
    return df


PROCESSORS = {
    "companies": process_companies,
    "contacts": process_contacts,
    "deals": process_deals,
    "marketing_events": process_marketing_events,
    "email_campaigns": process_email_campaigns,
    "support_tickets": process_support_tickets,
    "web_analytics": process_web_analytics,
}


# ── Phase 2: MapReduce Aggregations ────────────────────────────────

def aggregate_revenue_by_quarter(deals: pd.DataFrame) -> pd.DataFrame:
    return deals.groupby("quarter").agg(
        total_revenue=("amount", "sum"),
        weighted_pipeline=("weighted_amount", "sum"),
        avg_deal_size=("amount", "mean"),
        deal_count=("deal_id", "count"),
        win_count=("is_won", "sum"),
        loss_count=("is_lost", "sum"),
    ).reset_index()


def aggregate_revenue_by_month(deals: pd.DataFrame) -> pd.DataFrame:
    return deals.groupby("month").agg(
        total_revenue=("amount", "sum"),
        weighted_pipeline=("weighted_amount", "sum"),
        avg_deal_size=("amount", "mean"),
        deal_count=("deal_id", "count"),
        win_count=("is_won", "sum"),
        loss_count=("is_lost", "sum"),
    ).reset_index()


def aggregate_deals_by_region(deals: pd.DataFrame) -> pd.DataFrame:
    return deals.groupby("region").agg(
        total_revenue=("amount", "sum"),
        avg_deal_size=("amount", "mean"),
        deal_count=("deal_id", "count"),
        win_rate=("is_won", "mean"),
        avg_velocity=("velocity_score", "mean"),
    ).reset_index()


def aggregate_deals_by_industry(deals: pd.DataFrame) -> pd.DataFrame:
    return deals.groupby("industry").agg(
        total_revenue=("amount", "sum"),
        avg_deal_size=("amount", "mean"),
        deal_count=("deal_id", "count"),
        win_rate=("is_won", "mean"),
    ).reset_index()


def aggregate_pipeline_stages(deals: pd.DataFrame) -> pd.DataFrame:
    return deals.groupby("stage").agg(
        count=("deal_id", "count"),
        total_value=("amount", "sum"),
        avg_probability=("probability", "mean"),
        avg_days=("days_in_pipeline", "mean"),
    ).reset_index()


def aggregate_deals_by_pipeline(deals: pd.DataFrame) -> pd.DataFrame:
    return deals.groupby("pipeline").agg(
        total_revenue=("amount", "sum"),
        deal_count=("deal_id", "count"),
        win_rate=("is_won", "mean"),
        avg_deal_size=("amount", "mean"),
    ).reset_index()


def aggregate_marketing_by_channel(mktg: pd.DataFrame) -> pd.DataFrame:
    return mktg.groupby("channel").agg(
        event_count=("event_id", "count"),
        avg_session_duration=("session_duration_sec", "mean"),
        engagement_rate=("is_engaged", "mean"),
        avg_page_depth=("page_depth", "mean"),
    ).reset_index()


def aggregate_marketing_by_month(mktg: pd.DataFrame) -> pd.DataFrame:
    return mktg.groupby("month").agg(
        event_count=("event_id", "count"),
        engagement_rate=("is_engaged", "mean"),
        avg_session_duration=("session_duration_sec", "mean"),
    ).reset_index()


def aggregate_marketing_by_event_type(mktg: pd.DataFrame) -> pd.DataFrame:
    return mktg.groupby("event_type").agg(
        event_count=("event_id", "count"),
        avg_session_duration=("session_duration_sec", "mean"),
        engagement_rate=("is_engaged", "mean"),
    ).reset_index()


def aggregate_email_performance(emails: pd.DataFrame) -> pd.DataFrame:
    return emails.groupby("campaign_type").agg(
        total_sent=("email_event_id", "count"),
        open_rate=("is_opened", "mean"),
        click_rate=("is_clicked", "mean"),
        bounce_rate=("is_bounced", "mean"),
        unsub_rate=("is_unsub", "mean"),
    ).reset_index()


def aggregate_email_by_month(emails: pd.DataFrame) -> pd.DataFrame:
    return emails.groupby("month").agg(
        total_sent=("email_event_id", "count"),
        open_rate=("is_opened", "mean"),
        click_rate=("is_clicked", "mean"),
        bounce_rate=("is_bounced", "mean"),
    ).reset_index()


def aggregate_email_by_hour(emails: pd.DataFrame) -> pd.DataFrame:
    return emails.groupby("send_hour").agg(
        total_sent=("email_event_id", "count"),
        open_rate=("is_opened", "mean"),
        click_rate=("is_clicked", "mean"),
    ).reset_index()


def aggregate_contacts_by_lifecycle(contacts: pd.DataFrame) -> pd.DataFrame:
    return contacts.groupby("lifecycle_stage").agg(
        count=("contact_id", "count"),
        avg_lead_score=("lead_score", "mean"),
        avg_engagement=("engagement_index", "mean"),
    ).reset_index()


def aggregate_contacts_by_source(contacts: pd.DataFrame) -> pd.DataFrame:
    return contacts.groupby("lead_source").agg(
        count=("contact_id", "count"),
        avg_lead_score=("lead_score", "mean"),
        avg_engagement=("engagement_index", "mean"),
    ).reset_index()


def aggregate_support_by_category(tickets: pd.DataFrame) -> pd.DataFrame:
    return tickets.groupby("category").agg(
        ticket_count=("ticket_id", "count"),
        avg_resolution_hours=("resolution_hours", "mean"),
        sla_compliance=("sla_met", "mean"),
        avg_satisfaction=("satisfaction_score", "mean"),
    ).reset_index()


def aggregate_support_by_priority(tickets: pd.DataFrame) -> pd.DataFrame:
    return tickets.groupby("priority").agg(
        ticket_count=("ticket_id", "count"),
        avg_resolution_hours=("resolution_hours", "mean"),
        sla_compliance=("sla_met", "mean"),
        avg_satisfaction=("satisfaction_score", "mean"),
    ).reset_index()


def aggregate_web_by_page(web: pd.DataFrame) -> pd.DataFrame:
    return web.groupby("page_url").agg(
        sessions=("session_id", "count"),
        avg_duration=("session_duration_sec", "mean"),
        bounce_rate=("bounce", "mean"),
        conversion_rate=("conversion", "mean"),
        avg_page_views=("page_views", "mean"),
    ).reset_index()


def aggregate_web_by_country(web: pd.DataFrame) -> pd.DataFrame:
    return web.groupby("country").agg(
        sessions=("session_id", "count"),
        bounce_rate=("bounce", "mean"),
        conversion_rate=("conversion", "mean"),
        avg_duration=("session_duration_sec", "mean"),
    ).reset_index()


def aggregate_web_by_device(web: pd.DataFrame) -> pd.DataFrame:
    return web.groupby("device_type").agg(
        sessions=("session_id", "count"),
        bounce_rate=("bounce", "mean"),
        conversion_rate=("conversion", "mean"),
        avg_duration=("session_duration_sec", "mean"),
    ).reset_index()


def aggregate_web_by_month(web: pd.DataFrame) -> pd.DataFrame:
    return web.groupby("month").agg(
        sessions=("session_id", "count"),
        bounce_rate=("bounce", "mean"),
        conversion_rate=("conversion", "mean"),
        engagement_rate=("engaged_session", "mean"),
    ).reset_index()


def aggregate_companies_by_industry(companies: pd.DataFrame) -> pd.DataFrame:
    return companies.groupby("industry").agg(
        company_count=("company_id", "count"),
        avg_employees=("employee_count", "mean"),
        avg_revenue=("annual_revenue", "mean"),
        avg_traffic=("website_traffic_monthly", "mean"),
    ).reset_index()


def aggregate_companies_by_region(companies: pd.DataFrame) -> pd.DataFrame:
    return companies.groupby("region").agg(
        company_count=("company_id", "count"),
        avg_employees=("employee_count", "mean"),
        avg_revenue=("annual_revenue", "mean"),
    ).reset_index()


# ── Pipeline Orchestrator ──────────────────────────────────────────

def run_etl():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(AGGREGATED_DIR, exist_ok=True)

    total_start = time.time()
    print("=" * 65)
    print("  HubSpot ETL Pipeline")
    print("=" * 65)

    # Phase 1: Load & Transform
    print("\n─── Phase 1: Transform ───────────────────────────────────")
    processed = {}
    for name, proc_fn in PROCESSORS.items():
        raw_path = os.path.join(RAW_DIR, f"{name}.parquet")
        if not os.path.exists(raw_path):
            print(f"  ⚠ Skipping {name} (file not found)")
            continue
        t0 = time.time()
        df = pd.read_parquet(raw_path)
        df = proc_fn(df)
        out_path = os.path.join(PROCESSED_DIR, f"{name}.parquet")
        df.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)
        processed[name] = df
        elapsed = time.time() - t0
        print(f"  ✓ {name:<22s} {len(df):>12,} rows  ({elapsed:.1f}s)")

    # Phase 2: Aggregate
    print("\n─── Phase 2: Aggregate ──────────────────────────────────")
    aggregations = {
        "revenue_by_quarter": (aggregate_revenue_by_quarter, "deals"),
        "revenue_by_month": (aggregate_revenue_by_month, "deals"),
        "deals_by_region": (aggregate_deals_by_region, "deals"),
        "deals_by_industry": (aggregate_deals_by_industry, "deals"),
        "pipeline_stages": (aggregate_pipeline_stages, "deals"),
        "deals_by_pipeline": (aggregate_deals_by_pipeline, "deals"),
        "marketing_by_channel": (aggregate_marketing_by_channel, "marketing_events"),
        "marketing_by_month": (aggregate_marketing_by_month, "marketing_events"),
        "marketing_by_event_type": (aggregate_marketing_by_event_type, "marketing_events"),
        "email_performance": (aggregate_email_performance, "email_campaigns"),
        "email_by_month": (aggregate_email_by_month, "email_campaigns"),
        "email_by_hour": (aggregate_email_by_hour, "email_campaigns"),
        "contacts_by_lifecycle": (aggregate_contacts_by_lifecycle, "contacts"),
        "contacts_by_source": (aggregate_contacts_by_source, "contacts"),
        "support_by_category": (aggregate_support_by_category, "support_tickets"),
        "support_by_priority": (aggregate_support_by_priority, "support_tickets"),
        "web_by_page": (aggregate_web_by_page, "web_analytics"),
        "web_by_country": (aggregate_web_by_country, "web_analytics"),
        "web_by_device": (aggregate_web_by_device, "web_analytics"),
        "web_by_month": (aggregate_web_by_month, "web_analytics"),
        "companies_by_industry": (aggregate_companies_by_industry, "companies"),
        "companies_by_region": (aggregate_companies_by_region, "companies"),
    }

    for agg_name, (agg_fn, source) in aggregations.items():
        if source not in processed:
            continue
        t0 = time.time()
        result = agg_fn(processed[source])
        out_path = os.path.join(AGGREGATED_DIR, f"{agg_name}.parquet")
        result.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)
        elapsed = time.time() - t0
        print(f"  ✓ {agg_name:<30s} {len(result):>6} rows  ({elapsed:.2f}s)")

    elapsed = time.time() - total_start
    print()
    print("=" * 65)
    print(f"  ✅ Pipeline complete in {elapsed:.1f}s")
    print(f"  📁 Processed → {PROCESSED_DIR}")
    print(f"  📁 Aggregated → {AGGREGATED_DIR}")
    print("=" * 65)


if __name__ == "__main__":
    run_etl()
