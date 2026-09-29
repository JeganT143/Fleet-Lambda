"""Render every dashboard page against the running API and Airflow (read-only).

Nothing is clicked, so no DAG is triggered and no data changes.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("streamlit")

import streamlit as st  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

pytestmark = pytest.mark.integration

APP_FILE = str(Path(__file__).resolve().parents[2] / "fleet" / "dashboard" / "app.py")
PAGES = ["Overview", "Live Fleet", "Vehicles", "Daily Report", "Alerts", "Pipeline & Quality"]


@pytest.mark.parametrize("page", PAGES)
def test_page_renders_against_live_services(page):
    st.cache_resource.clear()
    at = AppTest.from_file(APP_FILE, default_timeout=60).run()
    if page != PAGES[0]:
        at.sidebar.radio[0].set_value(page).run()
    assert not at.exception, at.exception
    assert not at.error, [e.value for e in at.error]
    assert not at.warning, [w.value for w in at.warning]  # e.g. Airflow REST API not reachable
    if page == "Overview":
        metrics = {m.label: m.value for m in at.metric}
        assert metrics["API"] == "OK"
        assert metrics["PostgreSQL"] == "OK"
        assert metrics["Airflow scheduler"] == "HEALTHY"
