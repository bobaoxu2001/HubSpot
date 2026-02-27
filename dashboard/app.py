"""
Streamlit dashboard for the AI Brand Visibility Intelligence Engine.

Launch with:
    streamlit run dashboard/app.py

Requires a running PostgreSQL instance with populated tables.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.queries import (
    aisov_leaderboard,
    aisov_trend,
    cluster_distribution,
    competitor_displacement_rate,
    mention_rate_by_llm,
    risk_exposure_index,
    sentiment_distribution,
    visibility_by_intent,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="AI Brand Visibility Engine",
    page_icon="📊",
    layout="wide",
)

st.title("AI Brand Visibility Intelligence Engine")
st.markdown("Real-time analytics on how **HubSpot** surfaces across ChatGPT, Claude, and Perplexity.")

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------
brand = st.sidebar.text_input("Primary brand", value="HubSpot")
st.sidebar.markdown("---")
st.sidebar.markdown("**Navigation**")
section = st.sidebar.radio(
    "Section",
    [
        "Overview",
        "Mention Analysis",
        "Sentiment",
        "Competitive Landscape",
        "Risk Exposure",
        "Prompt Clusters",
    ],
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_df(data):
    """Convert query results to DataFrame, handling empty results."""
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

if section == "Overview":
    st.header("AISOV Overview")

    col1, col2, col3, col4 = st.columns(4)
    leaderboard = aisov_leaderboard()
    if leaderboard:
        row = next((r for r in leaderboard if r["brand_name"] == brand), leaderboard[0])
        col1.metric("AISOV Score", f"{float(row['aisov_score']):.3f}")
        col2.metric("Mention Rate", f"{float(row['mention_rate']) * 100:.1f}%")
        col3.metric("Positive Sentiment", f"{float(row['positive_sentiment_ratio']) * 100:.1f}%")
        col4.metric("Rec. Strength", f"{float(row['recommendation_strength_avg']):.3f}")

    # Trend chart
    trend_data = safe_df(aisov_trend(brand))
    if not trend_data.empty:
        st.subheader("AISOV Trend")
        fig = px.line(
            trend_data,
            x="period_start",
            y="aisov_score",
            markers=True,
            title="AI Share of Voice Over Time",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Intent heatmap
    intent_data = safe_df(visibility_by_intent(brand))
    if not intent_data.empty:
        st.subheader("Visibility by Intent Category")
        st.dataframe(intent_data, use_container_width=True)


elif section == "Mention Analysis":
    st.header("Mention Rate by LLM")
    mention_data = safe_df(mention_rate_by_llm(brand))
    if not mention_data.empty:
        fig = px.bar(
            mention_data,
            x="llm_name",
            y="mention_rate",
            color="llm_name",
            title=f"{brand} Mention Rate Across LLMs",
            text_auto=".1%",
        )
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(mention_data, use_container_width=True)
    else:
        st.info("No mention data available. Run the pipeline first.")


elif section == "Sentiment":
    st.header("Sentiment Distribution")
    sent_data = safe_df(sentiment_distribution(brand))
    if not sent_data.empty:
        fig = px.bar(
            sent_data,
            x="llm_name",
            y="count",
            color="sentiment",
            barmode="group",
            title=f"Sentiment Toward {brand} by LLM",
            color_discrete_map={
                "positive": "#2ecc71",
                "neutral": "#95a5a6",
                "negative": "#e74c3c",
            },
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(sent_data, use_container_width=True)
    else:
        st.info("No sentiment data available.")


elif section == "Competitive Landscape":
    st.header("Competitor Displacement Analysis")
    comp_data = safe_df(competitor_displacement_rate(brand))
    if not comp_data.empty:
        fig = px.bar(
            comp_data,
            x="competitor",
            y="displacement_rate",
            color="competitor",
            title="Competitor Displacement Rate (mentioned WITHOUT HubSpot)",
            text_auto=".1%",
        )
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(comp_data, use_container_width=True)
    else:
        st.info("No competitor displacement data available.")


elif section == "Risk Exposure":
    st.header("Risk Exposure Index")
    risk_data = safe_df(risk_exposure_index(brand))
    if not risk_data.empty:
        fig = px.bar(
            risk_data,
            x="llm_name",
            y="risk_index",
            color="llm_name",
            title="Risk Exposure by LLM (risk/criticism prompts)",
            text_auto=".3f",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(risk_data, use_container_width=True)
    else:
        st.info("No risk data available.")


elif section == "Prompt Clusters":
    st.header("Prompt Cluster Distribution")
    cluster_data = safe_df(cluster_distribution())
    if not cluster_data.empty:
        fig = px.pie(
            cluster_data,
            values="prompt_count",
            names="cluster_label",
            title="Prompts by Behavioural Cluster",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(cluster_data, use_container_width=True)
    else:
        st.info("No clustering data available. Run clustering first.")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption("AI Brand Visibility Intelligence Engine · Built for Fortune 500 analytics")
