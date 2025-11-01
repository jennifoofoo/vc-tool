"""Streamlit dashboard for VC-Sourcing-Tool."""

from __future__ import annotations

import altair as alt
import pandas as pd
import requests
import streamlit as st

STATS_URL = "http://localhost:8000/stats"


@st.cache_data(ttl=60)
def fetch_stats(url: str = STATS_URL) -> dict:
    """Fetch aggregated stats from FastAPI backend with caching."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        st.error(f"Failed to fetch stats from API: {exc}")
        return {}


def make_bar_chart(data: dict[str, int], title: str, top_n: int | None = None) -> alt.Chart | None:
    """Create an Altair bar chart from a mapping of label->count."""
    if not data:
        return None

    items = sorted(data.items(), key=lambda item: item[1], reverse=True)
    if top_n is not None:
        items = items[:top_n]

    df = pd.DataFrame(items, columns=["Label", "Count"])
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("Label", sort="-y"),
            y=alt.Y("Count", title="Count"),
            tooltip=["Label", "Count"],
        )
        .properties(title=title)
    )
    return chart


def render_news_tab(news_data: dict[str, object]) -> None:
    st.subheader("News Overview")

    total_news = news_data.get("total", 0)
    st.metric("Total News Items", f"{total_news:,}")

    by_source = news_data.get("by_source", {}) or {}
    chart = make_bar_chart(by_source, "News by Source")
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No news data available.")


def render_yc_tab(yc_data: dict[str, object]) -> None:
    st.subheader("YC Companies Overview")

    total_companies = yc_data.get("total", 0)
    st.metric("Total YC Companies", f"{total_companies:,}")

    batch_chart = make_bar_chart(yc_data.get("by_batch", {}) or {}, "Companies by Batch")
    if batch_chart is not None:
        st.altair_chart(batch_chart, use_container_width=True)
    else:
        st.info("No batch data available.")

    industry_chart = make_bar_chart(
        yc_data.get("by_industry", {}) or {},
        "Companies by Industry (Top 10)",
        top_n=10,
    )
    if industry_chart is not None:
        st.altair_chart(industry_chart, use_container_width=True)
    else:
        st.info("No industry data available.")

    status_chart = make_bar_chart(yc_data.get("by_status", {}) or {}, "Companies by Status")
    if status_chart is not None:
        st.altair_chart(status_chart, use_container_width=True)
    else:
        st.info("No status data available.")


def main() -> None:
    st.set_page_config(page_title="VC Sourcing Dashboard", layout="wide")
    st.title("🚀 VC Sourcing Dashboard")

    view_option = st.sidebar.selectbox("Select dataset", ["News", "YC Companies", "All"])

    stats = fetch_stats()
    news_stats = stats.get("news", {}) if isinstance(stats, dict) else {}
    yc_stats = stats.get("yc", {}) if isinstance(stats, dict) else {}

    tabs = st.tabs(["News", "YC Companies"])

    if view_option in ("News", "All"):
        with tabs[0]:
            render_news_tab(news_stats)
    else:
        with tabs[0]:
            st.info("News view hidden via sidebar.")

    if view_option in ("YC Companies", "All"):
        with tabs[1]:
            render_yc_tab(yc_stats)
    else:
        with tabs[1]:
            st.info("YC view hidden via sidebar.")

    st.caption("Data live from VC-Sourcing FastAPI backend.")


if __name__ == "__main__":
    main()

