"""
Global configuration for the HubSpot Big Data Analytics Platform.
"""
import os

# ─── Data Generation ───────────────────────────────────────────────
NUM_CONTACTS = 500_000
NUM_COMPANIES = 50_000
NUM_DEALS = 300_000
NUM_MARKETING_EVENTS = 1_000_000
NUM_SUPPORT_TICKETS = 200_000
NUM_EMAIL_CAMPAIGNS = 800_000
NUM_WEB_ANALYTICS = 2_000_000

# ─── Paths ─────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
AGGREGATED_DIR = os.path.join(DATA_DIR, "aggregated")

# ─── Pipeline ──────────────────────────────────────────────────────
CHUNK_SIZE = 50_000
NUM_PARTITIONS = 8
DATE_RANGE_START = "2023-01-01"
DATE_RANGE_END = "2025-12-31"

# ─── Dashboard ─────────────────────────────────────────────────────
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = int(os.environ.get("PORT", 5000))
DEBUG = os.environ.get("FLASK_DEBUG", "0") == "1"

# ─── Industry & Region Mappings ────────────────────────────────────
INDUSTRIES = [
    "Technology", "Healthcare", "Finance", "Manufacturing", "Retail",
    "Education", "Real Estate", "Media", "Energy", "Transportation",
    "Telecommunications", "Hospitality", "Agriculture", "Automotive",
    "Pharmaceuticals", "Aerospace", "Consulting", "Insurance",
    "Legal Services", "Non-Profit"
]

REGIONS = [
    "North America", "Europe", "Asia Pacific", "Latin America",
    "Middle East", "Africa", "Oceania"
]

DEAL_STAGES = [
    "Prospecting", "Qualification", "Proposal", "Negotiation",
    "Closed Won", "Closed Lost"
]

LEAD_SOURCES = [
    "Organic Search", "Paid Search", "Social Media", "Email Marketing",
    "Direct Traffic", "Referral", "Content Marketing", "Events",
    "Partner", "Cold Outreach"
]

CAMPAIGN_TYPES = [
    "Newsletter", "Product Launch", "Drip Campaign", "Re-engagement",
    "Seasonal Promotion", "Webinar Invite", "Case Study",
    "Feature Announcement", "Survey", "Onboarding"
]

TICKET_PRIORITIES = ["Critical", "High", "Medium", "Low"]
TICKET_CATEGORIES = [
    "Bug Report", "Feature Request", "Billing", "Integration",
    "Performance", "Security", "Onboarding", "Data Migration",
    "API Issue", "General Inquiry"
]
