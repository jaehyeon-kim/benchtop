"""The Streamlit dashboard: metric cards and revenue charts, updated from the WebSocket.

Run: python -m streamlit run sales/dashboard/streamlit_app.py
"""

import asyncio

import aiohttp
import streamlit as st
from streamlit_echarts import st_echarts

from sales.core.config import WS_URL
from sales.dashboard.metrics import LABELS, metric_cards, metrics, revenue_charts


def _draw(cards: list[dict], charts: list[dict], cards_area, charts_area) -> None:
    """Replaces the cards and charts on the page."""
    with cards_area.container():
        for col, card in zip(st.columns(len(cards)), cards):
            col.metric(**card)
    if not charts:  # before the first message
        return
    with charts_area.container():
        for col, options in zip(st.columns(len(charts)), charts):
            with col:
                st_echarts(options=options, height="500px")


async def _follow(cards_area, charts_area) -> None:
    """Redraws the page for every message the WebSocket sends."""
    previous = dict.fromkeys(LABELS, 0)
    async with aiohttp.ClientSession() as session, session.ws_connect(WS_URL) as ws:
        async for message in ws:
            records = message.json()
            current = metrics(records)
            _draw(
                metric_cards(current, previous),
                revenue_charts(records),
                cards_area,
                charts_area,
            )
            previous = current


st.set_page_config(page_title="theLook eCommerce", layout="wide")
st.title("theLook eCommerce Dashboard")
connect = st.checkbox("Connect to WS Server")
cards_area, charts_area = st.empty(), st.empty()
if connect:
    asyncio.run(_follow(cards_area, charts_area))
else:
    _draw(metric_cards(*[dict.fromkeys(LABELS, 0)] * 2), [], cards_area, charts_area)
