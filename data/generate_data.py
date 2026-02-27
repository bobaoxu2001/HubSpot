#!/usr/bin/env python3
"""
Large-scale synthetic data generator for HubSpot Big Data Analytics Platform.
Generates 4.85M+ records across 7 entity types using vectorized NumPy operations
and writes Parquet files in partitioned chunks for memory efficiency.
"""
import os
import sys
import time
import numpy as np
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.config import (
    NUM_CONTACTS, NUM_COMPANIES, NUM_DEALS, NUM_MARKETING_EVENTS,
    NUM_SUPPORT_TICKETS, NUM_EMAIL_CAMPAIGNS, NUM_WEB_ANALYTICS,
    RAW_DIR, DATE_RANGE_START, DATE_RANGE_END,
    INDUSTRIES, REGIONS, DEAL_STAGES, LEAD_SOURCES,
    CAMPAIGN_TYPES, TICKET_PRIORITIES, TICKET_CATEGORIES,
)


def _random_dates(start: str, end: str, n: int, rng: np.random.Generator) -> np.ndarray:
    ts_start = pd.Timestamp(start).value // 10**9
    ts_end = pd.Timestamp(end).value // 10**9
    return pd.to_datetime(rng.integers(ts_start, ts_end, size=n), unit="s")


def _print_progress(entity: str, count: int, elapsed: float):
    rate = count / elapsed if elapsed > 0 else 0
    print(f"  ✓ {entity:<22s} {count:>12,} records  ({elapsed:6.1f}s, {rate:,.0f} rec/s)")


def generate_companies(rng: np.random.Generator) -> pd.DataFrame:
    t0 = time.time()
    n = NUM_COMPANIES
    df = pd.DataFrame({
        "company_id": np.arange(1, n + 1),
        "company_name": [f"Company_{i:06d}" for i in range(1, n + 1)],
        "industry": rng.choice(INDUSTRIES, size=n),
        "region": rng.choice(REGIONS, size=n),
        "employee_count": rng.integers(5, 50000, size=n),
        "annual_revenue": np.round(rng.lognormal(mean=14, sigma=2, size=n), 2),
        "founded_year": rng.integers(1950, 2024, size=n),
        "website_traffic_monthly": rng.integers(100, 5_000_000, size=n),
        "created_at": _random_dates(DATE_RANGE_START, DATE_RANGE_END, n, rng),
    })
    _print_progress("Companies", n, time.time() - t0)
    return df


def generate_contacts(rng: np.random.Generator, company_ids: np.ndarray) -> pd.DataFrame:
    t0 = time.time()
    n = NUM_CONTACTS
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.io", "business.com",
               "tech.org", "work.net", "mail.com", "proton.me", "fastmail.com"]
    titles = ["CEO", "CTO", "VP Sales", "Marketing Manager", "Engineer",
              "Product Manager", "Designer", "Data Analyst", "Account Executive",
              "Customer Success", "DevOps Engineer", "HR Manager", "CFO",
              "Sales Rep", "Support Agent", "Consultant"]

    df = pd.DataFrame({
        "contact_id": np.arange(1, n + 1),
        "email": [f"user_{i}@{domains[i % len(domains)]}" for i in range(1, n + 1)],
        "company_id": rng.choice(company_ids, size=n),
        "job_title": rng.choice(titles, size=n),
        "lead_source": rng.choice(LEAD_SOURCES, size=n),
        "lead_score": np.clip(rng.normal(50, 25, size=n).astype(int), 0, 100),
        "lifecycle_stage": rng.choice(
            ["Subscriber", "Lead", "MQL", "SQL", "Opportunity", "Customer", "Evangelist"],
            size=n, p=[0.25, 0.20, 0.18, 0.12, 0.10, 0.10, 0.05]
        ),
        "region": rng.choice(REGIONS, size=n),
        "first_touch_date": _random_dates(DATE_RANGE_START, DATE_RANGE_END, n, rng),
        "num_page_views": rng.integers(0, 500, size=n),
        "num_form_submissions": rng.integers(0, 20, size=n),
    })
    _print_progress("Contacts", n, time.time() - t0)
    return df


def generate_deals(rng: np.random.Generator, contact_ids: np.ndarray, company_ids: np.ndarray) -> pd.DataFrame:
    t0 = time.time()
    n = NUM_DEALS
    stages = np.array(DEAL_STAGES)
    stage_idx = rng.choice(len(stages), size=n, p=[0.10, 0.15, 0.20, 0.15, 0.25, 0.15])

    df = pd.DataFrame({
        "deal_id": np.arange(1, n + 1),
        "contact_id": rng.choice(contact_ids, size=n),
        "company_id": rng.choice(company_ids, size=n),
        "deal_name": [f"Deal_{i:07d}" for i in range(1, n + 1)],
        "amount": np.round(rng.lognormal(mean=9, sigma=1.5, size=n), 2),
        "stage": stages[stage_idx],
        "pipeline": rng.choice(["Enterprise", "Mid-Market", "SMB", "Partner"], size=n,
                               p=[0.15, 0.25, 0.45, 0.15]),
        "probability": np.clip(rng.beta(2, 2, size=n) * 100, 0, 100).round(1),
        "days_in_pipeline": rng.integers(1, 365, size=n),
        "created_at": _random_dates(DATE_RANGE_START, DATE_RANGE_END, n, rng),
        "industry": rng.choice(INDUSTRIES, size=n),
        "region": rng.choice(REGIONS, size=n),
    })
    _print_progress("Deals", n, time.time() - t0)
    return df


def generate_marketing_events(rng: np.random.Generator, contact_ids: np.ndarray) -> pd.DataFrame:
    t0 = time.time()
    n = NUM_MARKETING_EVENTS
    event_types = ["Page View", "Form Submit", "CTA Click", "Email Open",
                   "Email Click", "Social Click", "Ad Click", "Video View",
                   "Download", "Webinar Attend"]

    df = pd.DataFrame({
        "event_id": np.arange(1, n + 1),
        "contact_id": rng.choice(contact_ids, size=n),
        "event_type": rng.choice(event_types, size=n),
        "channel": rng.choice(
            ["Organic", "Paid", "Social", "Email", "Direct", "Referral"],
            size=n, p=[0.25, 0.20, 0.18, 0.17, 0.12, 0.08]
        ),
        "campaign_id": rng.integers(1, 5001, size=n),
        "utm_source": rng.choice(["google", "facebook", "linkedin", "twitter",
                                    "bing", "newsletter", "partner", "direct"], size=n),
        "device_type": rng.choice(["Desktop", "Mobile", "Tablet"], size=n, p=[0.55, 0.35, 0.10]),
        "session_duration_sec": np.clip(rng.exponential(180, size=n).astype(int), 1, 3600),
        "page_depth": rng.integers(1, 20, size=n),
        "timestamp": _random_dates(DATE_RANGE_START, DATE_RANGE_END, n, rng),
    })
    _print_progress("Marketing Events", n, time.time() - t0)
    return df


def generate_email_campaigns(rng: np.random.Generator, contact_ids: np.ndarray) -> pd.DataFrame:
    t0 = time.time()
    n = NUM_EMAIL_CAMPAIGNS
    df = pd.DataFrame({
        "email_event_id": np.arange(1, n + 1),
        "contact_id": rng.choice(contact_ids, size=n),
        "campaign_id": rng.integers(1, 5001, size=n),
        "campaign_type": rng.choice(CAMPAIGN_TYPES, size=n),
        "action": rng.choice(
            ["Sent", "Delivered", "Opened", "Clicked", "Bounced", "Unsubscribed", "Spam"],
            size=n, p=[0.30, 0.28, 0.20, 0.10, 0.05, 0.04, 0.03]
        ),
        "subject_line_length": rng.integers(20, 120, size=n),
        "send_hour": rng.integers(0, 24, size=n),
        "send_day_of_week": rng.integers(0, 7, size=n),
        "timestamp": _random_dates(DATE_RANGE_START, DATE_RANGE_END, n, rng),
    })
    _print_progress("Email Campaigns", n, time.time() - t0)
    return df


def generate_support_tickets(rng: np.random.Generator, contact_ids: np.ndarray, company_ids: np.ndarray) -> pd.DataFrame:
    t0 = time.time()
    n = NUM_SUPPORT_TICKETS
    df = pd.DataFrame({
        "ticket_id": np.arange(1, n + 1),
        "contact_id": rng.choice(contact_ids, size=n),
        "company_id": rng.choice(company_ids, size=n),
        "category": rng.choice(TICKET_CATEGORIES, size=n),
        "priority": rng.choice(TICKET_PRIORITIES, size=n, p=[0.05, 0.15, 0.50, 0.30]),
        "status": rng.choice(["Open", "In Progress", "Waiting", "Resolved", "Closed"],
                             size=n, p=[0.15, 0.20, 0.10, 0.25, 0.30]),
        "resolution_hours": np.clip(rng.exponential(48, size=n), 0.5, 720).round(1),
        "satisfaction_score": rng.choice([1, 2, 3, 4, 5], size=n, p=[0.05, 0.10, 0.20, 0.35, 0.30]),
        "num_interactions": rng.integers(1, 30, size=n),
        "created_at": _random_dates(DATE_RANGE_START, DATE_RANGE_END, n, rng),
    })
    _print_progress("Support Tickets", n, time.time() - t0)
    return df


def generate_web_analytics(rng: np.random.Generator) -> pd.DataFrame:
    t0 = time.time()
    n = NUM_WEB_ANALYTICS
    pages = ["/", "/pricing", "/features", "/blog", "/contact", "/about",
             "/demo", "/docs", "/api", "/integrations", "/case-studies",
             "/careers", "/partners", "/resources", "/webinars",
             "/product/crm", "/product/marketing", "/product/sales",
             "/product/service", "/product/cms"]
    browsers = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
    countries = ["US", "UK", "DE", "FR", "CA", "AU", "JP", "BR", "IN", "MX",
                 "KR", "SG", "NL", "SE", "ES", "IT", "PL", "NG", "ZA", "AE"]

    df = pd.DataFrame({
        "session_id": np.arange(1, n + 1),
        "page_url": rng.choice(pages, size=n),
        "referrer_domain": rng.choice(
            ["google.com", "facebook.com", "linkedin.com", "twitter.com",
             "direct", "bing.com", "reddit.com", "youtube.com", "github.com", "other"],
            size=n
        ),
        "browser": rng.choice(browsers, size=n, p=[0.65, 0.18, 0.08, 0.07, 0.02]),
        "device_type": rng.choice(["Desktop", "Mobile", "Tablet"], size=n, p=[0.52, 0.38, 0.10]),
        "country": rng.choice(countries, size=n),
        "session_duration_sec": np.clip(rng.exponential(120, size=n).astype(int), 1, 1800),
        "page_views": rng.integers(1, 25, size=n),
        "bounce": rng.choice([0, 1], size=n, p=[0.55, 0.45]),
        "conversion": rng.choice([0, 1], size=n, p=[0.96, 0.04]),
        "timestamp": _random_dates(DATE_RANGE_START, DATE_RANGE_END, n, rng),
    })
    _print_progress("Web Analytics", n, time.time() - t0)
    return df


def _save_parquet(df: pd.DataFrame, name: str):
    path = os.path.join(RAW_DIR, f"{name}.parquet")
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"    → Saved {path} ({size_mb:.1f} MB)")


def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    rng = np.random.default_rng(seed=42)

    total_start = time.time()
    total_records = (NUM_CONTACTS + NUM_COMPANIES + NUM_DEALS +
                     NUM_MARKETING_EVENTS + NUM_SUPPORT_TICKETS +
                     NUM_EMAIL_CAMPAIGNS + NUM_WEB_ANALYTICS)

    print("=" * 65)
    print("  HubSpot Big Data Generator")
    print(f"  Target: {total_records:,} total records across 7 entities")
    print("=" * 65)
    print()

    # Generate in dependency order
    print("Phase 1: Core Entities")
    companies = generate_companies(rng)
    _save_parquet(companies, "companies")

    contacts = generate_contacts(rng, companies["company_id"].values)
    _save_parquet(contacts, "contacts")

    print("\nPhase 2: Transactional Data")
    deals = generate_deals(rng, contacts["contact_id"].values, companies["company_id"].values)
    _save_parquet(deals, "deals")

    tickets = generate_support_tickets(rng, contacts["contact_id"].values, companies["company_id"].values)
    _save_parquet(tickets, "support_tickets")

    print("\nPhase 3: Behavioral & Event Data")
    marketing = generate_marketing_events(rng, contacts["contact_id"].values)
    _save_parquet(marketing, "marketing_events")

    emails = generate_email_campaigns(rng, contacts["contact_id"].values)
    _save_parquet(emails, "email_campaigns")

    web = generate_web_analytics(rng)
    _save_parquet(web, "web_analytics")

    elapsed = time.time() - total_start
    print()
    print("=" * 65)
    print(f"  ✅ Generated {total_records:,} records in {elapsed:.1f}s")
    print(f"  📁 Output: {RAW_DIR}")
    print("=" * 65)


if __name__ == "__main__":
    main()
