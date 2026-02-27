#!/usr/bin/env python3
"""
Flask web server for the HubSpot Big Data Analytics Dashboard.
Serves the SPA frontend and provides JSON API endpoints.
"""
import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flask import Flask, render_template, jsonify
from src.analytics import (
    get_full_dashboard_data, get_kpi_summary,
    get_revenue_trend, get_deals_by_stage, get_deals_by_region,
    get_deals_by_industry, get_deals_by_pipeline,
    get_marketing_channels, get_marketing_trend, get_marketing_event_types,
    get_email_performance, get_email_trend, get_email_by_hour,
    get_contacts_lifecycle, get_contacts_by_source,
    get_support_by_category, get_support_by_priority,
    get_web_top_pages, get_web_by_country, get_web_by_device, get_web_trend,
    get_companies_by_industry, get_companies_by_region,
)
from src.config import DASHBOARD_HOST, DASHBOARD_PORT, DEBUG

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
)


# ── Pages ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── API ────────────────────────────────────────────────────────────

@app.route("/api/dashboard")
def api_dashboard():
    return jsonify(get_full_dashboard_data())


@app.route("/api/kpis")
def api_kpis():
    return jsonify(get_kpi_summary())


@app.route("/api/revenue-trend")
def api_revenue_trend():
    return jsonify(get_revenue_trend())


@app.route("/api/deals/stages")
def api_deals_stages():
    return jsonify(get_deals_by_stage())


@app.route("/api/deals/region")
def api_deals_region():
    return jsonify(get_deals_by_region())


@app.route("/api/deals/industry")
def api_deals_industry():
    return jsonify(get_deals_by_industry())


@app.route("/api/deals/pipeline")
def api_deals_pipeline():
    return jsonify(get_deals_by_pipeline())


@app.route("/api/marketing/channels")
def api_marketing_channels():
    return jsonify(get_marketing_channels())


@app.route("/api/marketing/trend")
def api_marketing_trend():
    return jsonify(get_marketing_trend())


@app.route("/api/marketing/events")
def api_marketing_events():
    return jsonify(get_marketing_event_types())


@app.route("/api/email/performance")
def api_email_performance():
    return jsonify(get_email_performance())


@app.route("/api/email/trend")
def api_email_trend():
    return jsonify(get_email_trend())


@app.route("/api/email/by-hour")
def api_email_by_hour():
    return jsonify(get_email_by_hour())


@app.route("/api/contacts/lifecycle")
def api_contacts_lifecycle():
    return jsonify(get_contacts_lifecycle())


@app.route("/api/contacts/source")
def api_contacts_source():
    return jsonify(get_contacts_by_source())


@app.route("/api/support/category")
def api_support_category():
    return jsonify(get_support_by_category())


@app.route("/api/support/priority")
def api_support_priority():
    return jsonify(get_support_by_priority())


@app.route("/api/web/pages")
def api_web_pages():
    return jsonify(get_web_top_pages())


@app.route("/api/web/country")
def api_web_country():
    return jsonify(get_web_by_country())


@app.route("/api/web/device")
def api_web_device():
    return jsonify(get_web_by_device())


@app.route("/api/web/trend")
def api_web_trend():
    return jsonify(get_web_trend())


@app.route("/api/companies/industry")
def api_companies_industry():
    return jsonify(get_companies_by_industry())


@app.route("/api/companies/region")
def api_companies_region():
    return jsonify(get_companies_by_region())


if __name__ == "__main__":
    app.run(host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=DEBUG)
