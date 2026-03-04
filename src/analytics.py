#!/usr/bin/env python3
"""
Analytics engine that reads aggregated Parquet data and produces
JSON-serialisable summaries consumed by the dashboard API.
"""
import os
import json
import numpy as np
import pandas as pd

from src.config import AGGREGATED_DIR, PROCESSED_DIR


def _load(name: str, directory: str = AGGREGATED_DIR) -> pd.DataFrame | None:
    path = os.path.join(directory, f"{name}.parquet")
    if not os.path.exists(path):
        return None
    return pd.read_parquet(path)


def _fmt(val, precision=2):
    if isinstance(val, (np.floating, float)):
        return round(float(val), precision)
    if isinstance(val, (np.integer, int)):
        return int(val)
    return val


# ── KPI Summary ─────────────────────────────────────────────────────

def get_kpi_summary() -> dict:
    """Top-level KPIs for the dashboard header."""
    deals = _load("deals", PROCESSED_DIR)
    contacts = _load("contacts", PROCESSED_DIR)
    companies = _load("companies", PROCESSED_DIR)
    web = _load("web_analytics", PROCESSED_DIR)
    tickets = _load("support_tickets", PROCESSED_DIR)
    emails = _load("email_campaigns", PROCESSED_DIR)

    total_revenue = float(deals["amount"].sum()) if deals is not None else 0
    won_revenue = float(deals.loc[deals["is_won"] == 1, "amount"].sum()) if deals is not None else 0
    pipeline_value = float(deals["weighted_amount"].sum()) if deals is not None else 0
    win_rate = float(deals["is_won"].mean()) if deals is not None else 0
    avg_deal = float(deals["amount"].mean()) if deals is not None else 0

    total_contacts = len(contacts) if contacts is not None else 0
    total_companies = len(companies) if companies is not None else 0
    total_deals = len(deals) if deals is not None else 0

    conversion_rate = float(web["conversion"].mean()) if web is not None else 0
    bounce_rate = float(web["bounce"].mean()) if web is not None else 0
    total_sessions = len(web) if web is not None else 0

    avg_csat = float(tickets["satisfaction_score"].mean()) if tickets is not None else 0
    sla_rate = float(tickets["sla_met"].mean()) if tickets is not None else 0
    total_tickets = len(tickets) if tickets is not None else 0

    email_open_rate = float(emails["is_opened"].mean()) if emails is not None else 0
    email_click_rate = float(emails["is_clicked"].mean()) if emails is not None else 0

    return {
        "total_revenue": _fmt(total_revenue),
        "won_revenue": _fmt(won_revenue),
        "pipeline_value": _fmt(pipeline_value),
        "win_rate": _fmt(win_rate * 100, 1),
        "avg_deal_size": _fmt(avg_deal),
        "total_contacts": total_contacts,
        "total_companies": total_companies,
        "total_deals": total_deals,
        "conversion_rate": _fmt(conversion_rate * 100, 2),
        "bounce_rate": _fmt(bounce_rate * 100, 1),
        "total_sessions": total_sessions,
        "avg_csat": _fmt(avg_csat, 1),
        "sla_compliance": _fmt(sla_rate * 100, 1),
        "total_tickets": total_tickets,
        "email_open_rate": _fmt(email_open_rate * 100, 1),
        "email_click_rate": _fmt(email_click_rate * 100, 1),
    }


# ── Chart Data Providers ───────────────────────────────────────────

def get_revenue_trend() -> dict:
    df = _load("revenue_by_month")
    if df is None:
        return {"labels": [], "datasets": []}
    df = df.sort_values("month")
    return {
        "labels": df["month"].tolist(),
        "datasets": [
            {"label": "Closed Revenue", "data": [_fmt(v) for v in df["total_revenue"]]},
            {"label": "Weighted Pipeline", "data": [_fmt(v) for v in df["weighted_pipeline"]]},
        ]
    }


def get_deals_by_stage() -> dict:
    df = _load("pipeline_stages")
    if df is None:
        return {"labels": [], "data": []}
    stage_order = ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
    df["_order"] = df["stage"].map({s: i for i, s in enumerate(stage_order)})
    df = df.sort_values("_order")
    return {
        "labels": df["stage"].tolist(),
        "counts": [int(v) for v in df["count"]],
        "values": [_fmt(v) for v in df["total_value"]],
    }


def get_deals_by_region() -> dict:
    df = _load("deals_by_region")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("total_revenue", ascending=False)
    return {
        "labels": df["region"].tolist(),
        "revenue": [_fmt(v) for v in df["total_revenue"]],
        "win_rate": [_fmt(v * 100, 1) for v in df["win_rate"]],
        "deal_count": [int(v) for v in df["deal_count"]],
    }


def get_deals_by_industry() -> dict:
    df = _load("deals_by_industry")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("total_revenue", ascending=False)
    return {
        "labels": df["industry"].tolist(),
        "revenue": [_fmt(v) for v in df["total_revenue"]],
        "win_rate": [_fmt(v * 100, 1) for v in df["win_rate"]],
        "deal_count": [int(v) for v in df["deal_count"]],
    }


def get_deals_by_pipeline() -> dict:
    df = _load("deals_by_pipeline")
    if df is None:
        return {"labels": [], "data": []}
    return {
        "labels": df["pipeline"].tolist(),
        "revenue": [_fmt(v) for v in df["total_revenue"]],
        "win_rate": [_fmt(v * 100, 1) for v in df["win_rate"]],
        "deal_count": [int(v) for v in df["deal_count"]],
    }


def get_marketing_channels() -> dict:
    df = _load("marketing_by_channel")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("event_count", ascending=False)
    return {
        "labels": df["channel"].tolist(),
        "event_count": [int(v) for v in df["event_count"]],
        "engagement_rate": [_fmt(v * 100, 1) for v in df["engagement_rate"]],
        "avg_duration": [_fmt(v, 0) for v in df["avg_session_duration"]],
    }


def get_marketing_trend() -> dict:
    df = _load("marketing_by_month")
    if df is None:
        return {"labels": [], "datasets": []}
    df = df.sort_values("month")
    return {
        "labels": df["month"].tolist(),
        "datasets": [
            {"label": "Events", "data": [int(v) for v in df["event_count"]]},
            {"label": "Engagement %", "data": [_fmt(v * 100, 1) for v in df["engagement_rate"]]},
        ]
    }


def get_marketing_event_types() -> dict:
    df = _load("marketing_by_event_type")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("event_count", ascending=False)
    return {
        "labels": df["event_type"].tolist(),
        "event_count": [int(v) for v in df["event_count"]],
        "engagement_rate": [_fmt(v * 100, 1) for v in df["engagement_rate"]],
    }


def get_email_performance() -> dict:
    df = _load("email_performance")
    if df is None:
        return {"labels": [], "data": []}
    return {
        "labels": df["campaign_type"].tolist(),
        "open_rate": [_fmt(v * 100, 1) for v in df["open_rate"]],
        "click_rate": [_fmt(v * 100, 1) for v in df["click_rate"]],
        "bounce_rate": [_fmt(v * 100, 1) for v in df["bounce_rate"]],
        "unsub_rate": [_fmt(v * 100, 2) for v in df["unsub_rate"]],
    }


def get_email_trend() -> dict:
    df = _load("email_by_month")
    if df is None:
        return {"labels": [], "datasets": []}
    df = df.sort_values("month")
    return {
        "labels": df["month"].tolist(),
        "datasets": [
            {"label": "Open Rate %", "data": [_fmt(v * 100, 1) for v in df["open_rate"]]},
            {"label": "Click Rate %", "data": [_fmt(v * 100, 1) for v in df["click_rate"]]},
        ]
    }


def get_email_by_hour() -> dict:
    df = _load("email_by_hour")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("send_hour")
    return {
        "labels": [f"{h:02d}:00" for h in df["send_hour"]],
        "open_rate": [_fmt(v * 100, 1) for v in df["open_rate"]],
        "click_rate": [_fmt(v * 100, 1) for v in df["click_rate"]],
    }


def get_contacts_lifecycle() -> dict:
    df = _load("contacts_by_lifecycle")
    if df is None:
        return {"labels": [], "data": []}
    stage_order = ["Subscriber", "Lead", "MQL", "SQL", "Opportunity", "Customer", "Evangelist"]
    df["_order"] = df["lifecycle_stage"].map({s: i for i, s in enumerate(stage_order)})
    df = df.sort_values("_order")
    return {
        "labels": df["lifecycle_stage"].tolist(),
        "count": [int(v) for v in df["count"]],
        "avg_score": [_fmt(v, 1) for v in df["avg_lead_score"]],
    }


def get_contacts_by_source() -> dict:
    df = _load("contacts_by_source")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("count", ascending=False)
    return {
        "labels": df["lead_source"].tolist(),
        "count": [int(v) for v in df["count"]],
        "avg_score": [_fmt(v, 1) for v in df["avg_lead_score"]],
    }


def get_support_by_category() -> dict:
    df = _load("support_by_category")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("ticket_count", ascending=False)
    return {
        "labels": df["category"].tolist(),
        "ticket_count": [int(v) for v in df["ticket_count"]],
        "avg_resolution": [_fmt(v, 1) for v in df["avg_resolution_hours"]],
        "sla_compliance": [_fmt(v * 100, 1) for v in df["sla_compliance"]],
        "satisfaction": [_fmt(v, 1) for v in df["avg_satisfaction"]],
    }


def get_support_by_priority() -> dict:
    df = _load("support_by_priority")
    if df is None:
        return {"labels": [], "data": []}
    order = ["Critical", "High", "Medium", "Low"]
    df["_order"] = df["priority"].map({s: i for i, s in enumerate(order)})
    df = df.sort_values("_order")
    return {
        "labels": df["priority"].tolist(),
        "ticket_count": [int(v) for v in df["ticket_count"]],
        "avg_resolution": [_fmt(v, 1) for v in df["avg_resolution_hours"]],
        "sla_compliance": [_fmt(v * 100, 1) for v in df["sla_compliance"]],
    }


def get_web_top_pages() -> dict:
    df = _load("web_by_page")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("sessions", ascending=False)
    return {
        "labels": df["page_url"].tolist(),
        "sessions": [int(v) for v in df["sessions"]],
        "bounce_rate": [_fmt(v * 100, 1) for v in df["bounce_rate"]],
        "conversion_rate": [_fmt(v * 100, 2) for v in df["conversion_rate"]],
    }


def get_web_by_country() -> dict:
    df = _load("web_by_country")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("sessions", ascending=False).head(15)
    return {
        "labels": df["country"].tolist(),
        "sessions": [int(v) for v in df["sessions"]],
        "conversion_rate": [_fmt(v * 100, 2) for v in df["conversion_rate"]],
    }


def get_web_by_device() -> dict:
    df = _load("web_by_device")
    if df is None:
        return {"labels": [], "data": []}
    return {
        "labels": df["device_type"].tolist(),
        "sessions": [int(v) for v in df["sessions"]],
        "bounce_rate": [_fmt(v * 100, 1) for v in df["bounce_rate"]],
        "conversion_rate": [_fmt(v * 100, 2) for v in df["conversion_rate"]],
    }


def get_web_trend() -> dict:
    df = _load("web_by_month")
    if df is None:
        return {"labels": [], "datasets": []}
    df = df.sort_values("month")
    return {
        "labels": df["month"].tolist(),
        "datasets": [
            {"label": "Sessions", "data": [int(v) for v in df["sessions"]]},
            {"label": "Conversion %", "data": [_fmt(v * 100, 2) for v in df["conversion_rate"]]},
        ]
    }


def get_companies_by_industry() -> dict:
    df = _load("companies_by_industry")
    if df is None:
        return {"labels": [], "data": []}
    df = df.sort_values("company_count", ascending=False)
    return {
        "labels": df["industry"].tolist(),
        "count": [int(v) for v in df["company_count"]],
        "avg_revenue": [_fmt(v) for v in df["avg_revenue"]],
    }


def get_companies_by_region() -> dict:
    df = _load("companies_by_region")
    if df is None:
        return {"labels": [], "data": []}
    return {
        "labels": df["region"].tolist(),
        "count": [int(v) for v in df["company_count"]],
        "avg_revenue": [_fmt(v) for v in df["avg_revenue"]],
    }


# ── Full dashboard payload ─────────────────────────────────────────

def get_full_dashboard_data() -> dict:
    return {
        "kpis": get_kpi_summary(),
        "revenue_trend": get_revenue_trend(),
        "deals_by_stage": get_deals_by_stage(),
        "deals_by_region": get_deals_by_region(),
        "deals_by_industry": get_deals_by_industry(),
        "deals_by_pipeline": get_deals_by_pipeline(),
        "marketing_channels": get_marketing_channels(),
        "marketing_trend": get_marketing_trend(),
        "marketing_event_types": get_marketing_event_types(),
        "email_performance": get_email_performance(),
        "email_trend": get_email_trend(),
        "email_by_hour": get_email_by_hour(),
        "contacts_lifecycle": get_contacts_lifecycle(),
        "contacts_by_source": get_contacts_by_source(),
        "support_by_category": get_support_by_category(),
        "support_by_priority": get_support_by_priority(),
        "web_top_pages": get_web_top_pages(),
        "web_by_country": get_web_by_country(),
        "web_by_device": get_web_by_device(),
        "web_trend": get_web_trend(),
        "companies_by_industry": get_companies_by_industry(),
        "companies_by_region": get_companies_by_region(),
    }
