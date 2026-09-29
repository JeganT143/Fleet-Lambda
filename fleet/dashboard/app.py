"""Fleet Lambda dashboard (Streamlit).

Run: streamlit run fleet/dashboard/app.py   (compose service `dashboard`, port 8501)

One page per part of the system, so every feature can be demonstrated from here:
  Overview          health of every layer, architecture, simulated clock
  Live Fleet        speed layer: event-time windows, vehicle map, zones (auto-refresh)
  Vehicles          one vehicle across both layers
  Daily Report      batch layer: reconciliation + profitability
  Alerts            threshold alerts
  Pipeline & Quality  Airflow runs, idempotent re-run, pipeline_runs, data quality, Kafka

The dashboard only presents data: it reads the FastAPI serving layer and uses the
Airflow REST API to show and trigger DAG runs. No business logic lives here.
"""

from __future__ import annotations

from datetime import UTC, datetime

import altair as alt
import pandas as pd
import streamlit as st

from fleet.common.config import load_settings
from fleet.common.contracts import BUSINESS_TIMEZONE, CURRENCY
from fleet.common.fleet_profiles import driver_name, profile_for, registration_plate, vehicle_model
from fleet.common.zones import CITY_NAME
from fleet.dashboard.client import (
    DAILY_BATCH_DAG,
    STREAM_ALERTS_DAG,
    ApiError,
    make_clients,
)

settings = load_settings()

STATUS_COLORS = {"idle": "#9e9e9e", "enroute": "#1e88e5", "on_trip": "#43a047"}
PROFIT_COLORS = {"profitable": "#2e7d32", "watch": "#f9a825", "unprofitable": "#c62828"}
SEVERITY_ICONS = {"critical": "🔴", "warning": "🟠", "info": "🔵"}
RUN_ICONS = {"success": "✅", "failed": "❌", "running": "⏳", "queued": "⏳", "skipped": "⏭️"}

PAGES = [
    "Overview",
    "Live Fleet",
    "Vehicles",
    "Daily Report",
    "Alerts",
    "Pipeline & Quality",
]


# ============================================================================ helpers
@st.cache_resource
def clients():
    return make_clients(settings)


def api_get(path: str, **params):
    """Fetch from the API; show a clear error and stop the page if it fails."""
    api, _ = clients()
    try:
        return api.get(path, params=params)
    except ApiError as exc:
        st.error(f"Could not load `{path}`: {exc}")
        st.stop()


def money(value: float | None) -> str:
    return "–" if value is None else f"{CURRENCY} {value:,.0f}"


def ts(value: str | None) -> str:
    """ISO timestamp (UTC) -> 'YYYY-MM-DD HH:MM:SS' in Sri Lanka time."""
    if not value:
        return "–"
    return pd.Timestamp(value).tz_convert(BUSINESS_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")


def with_identity(df: pd.DataFrame) -> pd.DataFrame:
    """Add number plate, car model and profile next to vehicle_id (display only)."""
    df = df.copy()
    at = df.columns.get_loc("vehicle_id") + 1
    df.insert(at, "plate", df["vehicle_id"].map(registration_plate))
    df.insert(at + 1, "model", df["vehicle_id"].map(vehicle_model))
    df.insert(at + 2, "profile", df["vehicle_id"].map(lambda v: profile_for(v).name))
    return df


def seconds_ago(value: str | None) -> float | None:
    if not value:
        return None
    return (datetime.now(UTC) - pd.Timestamp(value).to_pydatetime()).total_seconds()


def status_chart(df: pd.DataFrame, x: str, y: str, color_field: str, colors: dict[str, str]):
    """Bar chart whose bar colours follow a fixed status -> colour mapping."""
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{x}:N", sort=None, title=None),
            y=alt.Y(f"{y}:Q"),
            color=alt.Color(
                f"{color_field}:N",
                scale=alt.Scale(domain=list(colors), range=list(colors.values())),
                legend=alt.Legend(orient="top", title=None),
            ),
            tooltip=list(df.columns),
        )
    )


# ============================================================================ pages
def page_overview() -> None:
    st.title(f"Fleet Lambda: ride-hailing operations in {CITY_NAME}, Sri Lanka")
    st.caption(
        "Lambda architecture demo: live telemetry (speed layer) + daily expense files "
        "(batch layer), reconciled into a per-vehicle profitability report. "
        f"Money in {CURRENCY}, times in Sri Lanka time (UTC+05:30). "
        "1 simulated business day = 5 real minutes."
    )

    api, airflow = clients()
    # --- health of every component -------------------------------------------------
    cols = st.columns(5)
    try:
        health = api.get("/health")
        cols[0].metric("API", "OK")
        cols[1].metric("PostgreSQL", health["database"].upper())
    except ApiError:
        cols[0].metric("API", "DOWN")
        cols[1].metric("PostgreSQL", "unknown")
        st.error("The API is not reachable; start it with `make up-app`.")
        st.stop()
    try:
        af = airflow.health()
        cols[2].metric("Airflow scheduler", af["scheduler"]["status"].upper())
    except (ApiError, KeyError):
        cols[2].metric("Airflow scheduler", "DOWN")

    summary = api_get("/api/v1/fleet/summary")
    age = seconds_ago(summary["last_ingestion_ts"])
    fresh = age is not None and age < settings.alert_no_data_seconds
    cols[3].metric(
        "Stream (Kafka → Spark)",
        "FLOWING" if fresh else "STALLED",
        f"last event {age:.0f}s ago" if age is not None else "no data yet",
        delta_color="normal" if fresh else "inverse",
    )
    cols[4].metric("Open alerts", summary["open_alerts"])

    # --- simulated clock ------------------------------------------------------------
    st.subheader("Simulated clock")
    c1, c2, c3 = st.columns(3)
    c1.metric("Simulated time in Colombo (latest event)", ts(summary["last_event_timestamp"]))
    totals = summary["business_day_totals"] or {}
    c2.metric("Current business date", totals.get("business_date", "–"))
    reports = api_get("/api/v1/reports/daily")["reports"]
    c3.metric("Days reconciled by the batch layer", len(reports))

    # --- architecture -----------------------------------------------------------------
    st.subheader("Architecture")
    st.graphviz_chart(
        """
        digraph {
          rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#f5f5f5", fontsize=11];
          subgraph cluster_speed { label="Speed layer"; color="#1e88e5";
            producer [label="Python producer\\n(simulator)"];
            kafka [label="Kafka\\nvehicle.telemetry\\n3 partitions\\nkey=vehicle_id",
                   shape=cylinder];
            spark [label="Spark Structured Streaming\\nvalidate · enrich · 1h windows"];
          }
          subgraph cluster_batch { label="Batch layer"; color="#43a047";
            gen [label="Expense generator"];
            files [label="Landing CSV\\n(one per day)", shape=note];
            airflow [label="Airflow\\nfleet_daily_batch"];
            sparkb [label="Spark batch\\nload + reconcile"];
          }
          pg [label="PostgreSQL\\n(serving layer)", shape=cylinder, fillcolor="#fff3e0"];
          api [label="FastAPI"]; ui [label="This dashboard", fillcolor="#e3f2fd"];
          producer -> kafka -> spark -> pg;
          gen -> files -> airflow -> sparkb -> pg;
          pg -> sparkb [label="stream history", style=dashed];
          pg -> api -> ui;
          airflow -> pg [label="alerts", style=dashed];
        }
        """
    )

    st.subheader("Where to see each feature")
    st.markdown(
        """
| Feature | Page |
|---|---|
| Continuous Kafka ingestion, event-time windows, idle ratio, zones | **Live Fleet** |
| Vehicle history across the speed and batch layers | **Vehicles** |
| Airflow + Spark batch, reconciliation, utilisation, profitability | **Daily Report** |
| Threshold alerts (no data, idle vehicle, low profit) | **Alerts** |
| Airflow runs, idempotent re-run, data quality, Kafka partitions | **Pipeline & Quality** |
        """
    )
    st.markdown(
        f"Other tools: [API docs (Swagger)]({settings.api_public_url}/docs) · "
        f"[Airflow UI]({settings.airflow_public_url})"
    )


def page_live_fleet() -> None:
    st.title("Live fleet (speed layer)")
    st.caption(
        "Kafka → Spark Structured Streaming → PostgreSQL. Metrics are 1-hour tumbling "
        "windows on **simulated event time** with a 15-minute watermark; a new window "
        "starts about every 12 real seconds."
    )
    auto = st.toggle("Auto-refresh every 10 s", value=True)

    @st.fragment(run_every="10s" if auto else None)
    def live() -> None:
        summary = api_get("/api/v1/fleet/summary")
        if not summary["data_available"]:
            st.info(summary.get("message") or "Waiting for the first streaming window…")
            return
        w = summary["latest_window"]
        day = summary["business_day_totals"]
        st.markdown(
            f"**Latest window:** {ts(w['window_start'])} → {ts(w['window_end'])} "
            f"(business date {w['business_date']}, Sri Lanka time; "
            f"updated {ts(w['updated_at'])})"
        )
        c = st.columns(6)
        c[0].metric("Vehicles reporting", w["vehicles_reporting"])
        c[1].metric("Active vehicles", w["active_vehicles"])
        c[2].metric("Idle vehicles", w["idle_vehicles"])
        c[3].metric("Idle ratio", f"{w['idle_ratio']:.0%}")
        c[4].metric("Trips / hour", f"{w['trips_per_hour']:.0f}")
        c[5].metric("Earnings (window)", money(w["total_earnings"]))
        c = st.columns(4)
        c[0].metric("Events / hour", f"{w['events_per_hour']:.0f}")
        c[1].metric("Avg fare (window)", money(w["avg_fare"]))
        c[2].metric(f"Trips today ({day['business_date']})", day["trips_completed"])
        c[3].metric("Earnings today", money(day["total_earnings"]))

        left, right = st.columns([3, 2])
        with left:
            st.subheader("Vehicle positions (latest event)")
            states = api_get("/api/v1/vehicles")
            vdf = pd.DataFrame(states["vehicles"])
            vdf["color"] = vdf["status"].map(STATUS_COLORS)
            st.map(vdf, latitude="latitude", longitude="longitude", color="color", size=120)
            st.caption(
                " · ".join(
                    f"<span style='color:{STATUS_COLORS[s]}'>●</span> {s}: "
                    f"{states['status_counts'].get(s, 0)}"
                    for s in STATUS_COLORS
                ),
                unsafe_allow_html=True,
            )
        with right:
            st.subheader("Earnings by zone (today)")
            zones = api_get("/api/v1/fleet/zones")["zones"]
            if zones:
                zdf = pd.DataFrame(zones)
                st.bar_chart(zdf, x="zone", y="total_earnings", horizontal=True)
                st.dataframe(
                    zdf[["zone", "trips_completed", "total_earnings", "avg_fare"]],
                    hide_index=True,
                    column_config={
                        "total_earnings": st.column_config.NumberColumn(format="LKR %.0f"),
                        "avg_fare": st.column_config.NumberColumn(format="LKR %.0f"),
                    },
                )

        st.subheader("Recent windows")
        windows = api_get("/api/v1/fleet/windows", limit=48)["windows"]
        wdf = pd.DataFrame(windows)
        wdf["window_start"] = pd.to_datetime(wdf["window_start"])
        c1, c2 = st.columns(2)
        with c1:
            st.caption("Active vs idle vehicles per window")
            st.line_chart(wdf, x="window_start", y=["active_vehicles", "idle_vehicles"])
        with c2:
            st.caption(f"Earnings per window ({CURRENCY})")
            st.bar_chart(wdf, x="window_start", y="total_earnings")
        c1, c2 = st.columns(2)
        with c1:
            st.caption("Idle ratio per window")
            st.line_chart(wdf, x="window_start", y="idle_ratio")
        with c2:
            st.caption("Trips completed per window")
            st.bar_chart(wdf, x="window_start", y="trips_completed")

    live()

    with st.expander("How are these metrics computed?"):
        st.markdown(
            """
- **idle ratio** = idle events ÷ all events in the window. Every vehicle reports at a fixed
  cadence, so the share of events equals the share of time.
- **active vehicles** = distinct vehicles with at least one `enroute`/`on_trip` event
  (HyperLogLog `approx_count_distinct`, exact for this fleet size); **idle** = reporting − active.
- **trips** = events with `fare > 0` (the fare is sent once, on the last `on_trip` event);
  **earnings** = `SUM(fare)`.
- Late events beyond the watermark are left out of these windows but still stored, and
  the daily batch recomputes everything from the full history.
            """
        )


def page_vehicles() -> None:
    st.title("Vehicle detail")
    states = api_get("/api/v1/vehicles")["vehicles"]
    if not states:
        st.info("No vehicles seen yet.")
        return
    ids = sorted(v["vehicle_id"] for v in states)
    vehicle_id = st.selectbox(
        "Vehicle",
        ids,
        format_func=lambda v: f"{v} · {registration_plate(v)} · {vehicle_model(v)}",
    )
    st.caption(
        f"Number plate **{registration_plate(vehicle_id)}** · {vehicle_model(vehicle_id)} · "
        f"profile **{profile_for(vehicle_id).name.replace('_', ' ')}**"
    )
    data = api_get(f"/api/v1/vehicles/{vehicle_id}")
    ev, stats = data["latest_event"], data["stream_stats"]

    st.subheader("Right now (speed layer)")
    if ev:
        c = st.columns(5)
        c[0].metric("Status", ev["status"])
        c[1].metric("Zone", ev["zone"])
        c[2].metric("Speed", f"{ev['speed']:.0f} km/h")
        c[3].metric("Driver", driver_name(ev["driver_id"]))
        c[4].metric("Last event (Colombo time)", ts(ev["event_timestamp"])[11:])
    if stats:
        c = st.columns(3)
        c[0].metric(f"Events on {stats['business_date']}", stats["event_count"])
        c[1].metric("Trips so far", stats["trips_completed"])
        c[2].metric("Earnings so far", money(stats["earnings"]))

    st.subheader("Last 7 days (batch layer)")
    daily = data["daily_profitability"]
    if daily:
        ddf = pd.DataFrame(daily).sort_values("business_date")
        st.altair_chart(
            status_chart(
                ddf[["business_date", "estimated_profit", "profitability_status"]],
                "business_date",
                "estimated_profit",
                "profitability_status",
                PROFIT_COLORS,
            )
        )
        st.dataframe(
            ddf[
                [
                    "business_date",
                    "trips",
                    "earnings",
                    "fuel_cost",
                    "maintenance_cost",
                    "estimated_profit",
                    "utilization_rate",
                    "service_flag",
                    "profitability_status",
                ]
            ],
            hide_index=True,
            column_config=_money_columns() | _util_column(),
        )
    else:
        st.info("No reconciled days yet for this vehicle.")

    st.subheader("Open alerts")
    if data["open_alerts"]:
        st.dataframe(pd.DataFrame(data["open_alerts"])[["alert_type", "severity", "message"]])
    else:
        st.success("No open alerts.")


def _money_columns() -> dict:
    fmt = st.column_config.NumberColumn(format="LKR %.0f")
    return {
        c: fmt
        for c in (
            "earnings",
            "fuel_cost",
            "maintenance_cost",
            "total_operating_cost",
            "estimated_profit",
        )
    }


def _util_column() -> dict:
    return {
        "utilization_rate": st.column_config.ProgressColumn(
            "utilization", min_value=0.0, max_value=1.0, format="%.2f"
        )
    }


def page_daily_report() -> None:
    st.title("Daily profitability report (batch layer)")
    st.caption(
        "Daily expense CSV → Airflow → Spark batch → reconciled with that day's stream "
        "history in PostgreSQL."
    )
    reports = api_get("/api/v1/reports/daily")["reports"]
    if not reports:
        st.info(
            "No day has been reconciled yet. The first report appears ~6–8 minutes after "
            "start (a simulated day must finish, then the batch DAG runs)."
        )
        return
    day = st.selectbox("Business date", [r["business_date"] for r in reports])
    report = api_get(f"/api/v1/reports/daily/{day}")
    s = report["summary"]

    c = st.columns(5)
    c[0].metric("Vehicles", s["vehicle_count"])
    c[1].metric("Trips", s["total_trips"])
    c[2].metric("Earnings", money(s["total_earnings"]))
    c[3].metric("Operating cost", money(s["total_operating_cost"]))
    c[4].metric("Estimated profit", money(s["total_estimated_profit"]))
    c = st.columns(4)
    c[0].metric("Avg utilisation", f"{s['avg_utilization_rate']:.1%}")
    for i, status in enumerate(PROFIT_COLORS, start=1):
        c[i].metric(status.capitalize(), s["status_counts"].get(status, 0))

    vdf = with_identity(pd.DataFrame(report["vehicles"]))

    st.subheader("Estimated profit per vehicle")
    st.altair_chart(
        status_chart(
            vdf[["vehicle_id", "estimated_profit", "profitability_status", "profile"]],
            "vehicle_id",
            "estimated_profit",
            "profitability_status",
            PROFIT_COLORS,
        )
    )
    st.dataframe(
        vdf[
            [
                "vehicle_id",
                "plate",
                "model",
                "profile",
                "trips",
                "earnings",
                "fuel_cost",
                "maintenance_cost",
                "total_operating_cost",
                "estimated_profit",
                "utilization_rate",
                "service_flag",
                "profitability_status",
            ]
        ],
        hide_index=True,
        column_config=_money_columns() | _util_column(),
    )

    st.subheader("Reconciliation: telemetry vs expense system")
    st.caption(
        "Distance estimated from telemetry (speed × time between events) next to the "
        "odometer distance in the expense file. They are independent sources, so gaps "
        "are expected; a large gap is worth investigating."
    )
    dist = vdf.melt(
        id_vars="vehicle_id",
        value_vars=["stream_distance_km", "distance_km"],
        var_name="source",
        value_name="km",
    )
    dist["source"] = dist["source"].map(
        {"stream_distance_km": "telemetry (stream)", "distance_km": "odometer (batch)"}
    )
    st.altair_chart(
        alt.Chart(dist)
        .mark_bar()
        .encode(
            x=alt.X("vehicle_id:N", title=None),
            xOffset="source:N",
            y="km:Q",
            color=alt.Color("source:N", legend=alt.Legend(orient="top", title=None)),
            tooltip=["vehicle_id", "source", "km"],
        )
    )

    with st.expander("Formulas and thresholds"):
        st.markdown(
            f"""
```
trips                = COUNT(events with fare > 0)
earnings             = SUM(fare)
total_operating_cost = fuel_cost + maintenance_cost
estimated_profit     = earnings − fuel_cost − maintenance_cost
utilization_rate     = on_trip events / all events of the vehicle that day
status               = profitable   if profit ≥ {settings.profitable_min_profit:,.0f}
                       watch        if profit ≥ {settings.watch_min_profit:,.0f}
                       unprofitable otherwise
```
Vehicle profiles (shared by both simulators): V003/V013 low utilisation (Suzuki Alto),
V006/V016 high fuel cost (Toyota HiAce vans), V009/V019 maintenance heavy (old Corollas),
the rest normal. All amounts in {CURRENCY}.
            """
        )

    run = report["latest_pipeline_run"]
    if run:
        st.caption(
            f"Latest pipeline run: {RUN_ICONS.get(run['status'], '')} **{run['pipeline_name']}** "
            f"#{run['run_id']}, {run['status']}, rows read {run['rows_read']}, "
            f"written {run['rows_written']}, finished {ts(run['finished_at'])}"
        )


def page_alerts() -> None:
    st.title("Alerts")
    rules = [
        (
            "no_stream_data",
            f"no event ingested for {settings.alert_no_data_seconds} real seconds",
            f"{STREAM_ALERTS_DAG} (every minute)",
        ),
        (
            "vehicle_idle",
            f"idle for {settings.alert_idle_minutes} simulated minutes",
            f"{STREAM_ALERTS_DAG} (every minute)",
        ),
        (
            "low_profitability",
            f"daily profit below {money(settings.alert_min_daily_profit)}",
            f"{DAILY_BATCH_DAG} (after reconciliation)",
        ),
    ]
    st.dataframe(pd.DataFrame(rules, columns=["alert", "rule", "raised by"]), hide_index=True)
    st.markdown(
        """
Each alert has a unique de-duplication key, so the same condition is never raised twice.
Alerts resolve automatically when the condition clears. To demo `no_stream_data`, run
`docker compose stop producer`, wait ~90 s, then `docker compose --profile app up -d producer`.
        """
    )
    c1, c2, c3 = st.columns(3)
    status = c1.selectbox("Status", ["open", "resolved", "(all)"])
    alert_type = c2.selectbox(
        "Type", ["(all)", "no_stream_data", "vehicle_idle", "low_profitability"]
    )
    vehicle = c3.text_input("Vehicle id (e.g. V006)").strip().upper()
    data = api_get(
        "/api/v1/alerts",
        status=None if status == "(all)" else status,
        alert_type=None if alert_type == "(all)" else alert_type,
        vehicle_id=vehicle or None,
        limit=500,
    )
    alerts = pd.DataFrame(data["alerts"])
    if alerts.empty:
        st.success("No alerts match these filters.")
        return
    counts = alerts.groupby(["alert_type", "severity"]).size().rename("alerts").reset_index()
    st.altair_chart(
        alt.Chart(counts)
        .mark_bar()
        .encode(
            x=alt.X("alert_type:N", title=None),
            y="alerts:Q",
            color=alt.Color(
                "severity:N",
                scale=alt.Scale(
                    domain=["critical", "warning", "info"], range=["#c62828", "#fb8c00", "#1e88e5"]
                ),
            ),
            tooltip=["alert_type", "severity", "alerts"],
        )
    )
    alerts.insert(0, "", alerts["severity"].map(SEVERITY_ICONS))
    alerts.insert(
        alerts.columns.get_loc("vehicle_id") + 1,
        "plate",
        # fleet-wide alerts (no_stream_data) have no vehicle: pandas gives NaN there
        alerts["vehicle_id"].map(lambda v: registration_plate(v) if isinstance(v, str) else ""),
    )
    alerts["created_at"] = alerts["created_at"].map(ts)
    alerts["resolved_at"] = alerts["resolved_at"].map(ts)
    st.dataframe(
        alerts[
            [
                "",
                "alert_id",
                "alert_type",
                "severity",
                "vehicle_id",
                "plate",
                "business_date",
                "message",
                "status",
                "created_at",
                "resolved_at",
            ]
        ],
        hide_index=True,
    )


def page_pipeline() -> None:
    st.title("Pipeline & data quality")
    _, airflow = clients()

    # --- Airflow ----------------------------------------------------------------------
    st.subheader("Airflow DAG runs")
    st.markdown(
        f"Open the [Airflow UI]({settings.airflow_public_url}) for task logs and the graph."
    )
    try:
        c1, c2 = st.columns(2)
        for col, dag in ((c1, DAILY_BATCH_DAG), (c2, STREAM_ALERTS_DAG)):
            runs = airflow.dag_runs(dag, limit=8)
            col.markdown(f"**{dag}**")
            col.dataframe(
                pd.DataFrame(
                    [
                        {
                            "": RUN_ICONS.get(r["state"], ""),
                            "run": r["dag_run_id"],
                            "state": r["state"],
                            "business_date": (r.get("conf") or {}).get("business_date", ""),
                            "started": ts(r.get("start_date")),
                        }
                        for r in runs
                    ]
                ),
                hide_index=True,
            )
    except ApiError as exc:
        st.warning(f"Airflow REST API not available: {exc}")

    # --- Idempotency demo -----------------------------------------------------------------
    st.subheader("Idempotency demo: re-run a business date")
    st.caption(
        "Triggers `fleet_daily_batch` for a date that was already processed. The batch "
        "replaces that date's rows in one transaction, so the report must be identical "
        "afterwards: same row count, same total profit, one more successful run."
    )
    reports = api_get("/api/v1/reports/daily")["reports"]
    if reports:
        c1, c2 = st.columns([2, 1])
        day = c1.selectbox("Date to re-run", [r["business_date"] for r in reports])
        if c2.button("Re-run batch for this date", type="primary"):
            before = api_get(f"/api/v1/reports/daily/{day}")["summary"]
            run_id = f"dashboard_rerun_{day}_{datetime.now(UTC):%H%M%S}"
            try:
                airflow.trigger(DAILY_BATCH_DAG, {"business_date": day}, run_id)
                st.session_state["rerun"] = {"day": day, "run_id": run_id, "before": before}
            except ApiError as exc:
                st.error(f"Could not trigger the DAG: {exc}")
        if "rerun" in st.session_state:
            _rerun_status()

    # --- pipeline_runs -----------------------------------------------------------------
    st.subheader("Batch job history (`pipeline_runs`)")
    runs = pd.DataFrame(api_get("/api/v1/pipeline/runs", limit=30)["runs"])
    if not runs.empty:
        runs.insert(0, "", runs["status"].map(RUN_ICONS))
        runs["duration_s"] = (
            (pd.to_datetime(runs["finished_at"]) - pd.to_datetime(runs["started_at"]))
            .dt.total_seconds()
            .round(1)
        )
        st.dataframe(
            runs[
                [
                    "",
                    "run_id",
                    "pipeline_name",
                    "business_date",
                    "status",
                    "rows_read",
                    "rows_written",
                    "rows_rejected",
                    "duration_s",
                    "error_message",
                ]
            ],
            hide_index=True,
        )

    # --- data quality ------------------------------------------------------------------
    st.subheader("Streaming data quality")
    dq = api_get("/api/v1/data-quality")
    c = st.columns(4)
    c[0].metric("Records checked", f"{dq['stream_records_total']:,}")
    c[1].metric("Valid", f"{dq['stream_records_valid']:,}")
    c[2].metric("Quarantined", f"{dq['stream_records_rejected']:,}")
    c[3].metric("Rejection rate", f"{dq['stream_rejection_rate']:.2%}")
    st.caption(
        "The producer injects ~0.5% invalid records on purpose. Spark routes them to "
        "`rejected_events` with a reason code and the stream keeps running."
    )
    c1, c2 = st.columns(2)
    with c1:
        st.caption("Quarantined records by reason")
        reasons = pd.DataFrame(
            list(dq["rejected_by_reason"].items()), columns=["reason", "records"]
        )
        if not reasons.empty:
            st.bar_chart(reasons, x="reason", y="records", horizontal=True)
    with c2:
        st.caption("Kafka partitions (key = vehicle_id)")
        parts = pd.DataFrame(dq["kafka_partitions"])
        if not parts.empty:
            parts["kafka_partition"] = "partition " + parts["kafka_partition"].astype(str)
            st.bar_chart(parts, x="kafka_partition", y="event_count")
            st.dataframe(parts, hide_index=True)
    with st.expander("Latest quarantined records"):
        st.dataframe(pd.DataFrame(dq["recent_rejected"]), hide_index=True)

    st.subheader("Batch file validation")
    checks = pd.DataFrame(dq["recent_batch_checks"])
    if not checks.empty:
        checks["rule_failures"] = checks["rule_failures"].map(lambda d: d or "–")
        st.dataframe(checks, hide_index=True)
    st.caption(
        "An invalid expense file fails `validate_expenses` in Airflow, loads nothing and "
        "is recorded as a failed run. Demo: "
        "`docker compose exec expense-generator python -m fleet.batch.generator "
        "--business-date 2099-01-05 --corrupt`, then trigger the DAG with that date."
    )


@st.fragment(run_every="3s")
def _rerun_status() -> None:
    """Poll the triggered DAG run and compare the report before and after."""
    _, airflow = clients()
    info = st.session_state["rerun"]
    try:
        run = airflow.dag_run(DAILY_BATCH_DAG, info["run_id"])
        tasks = airflow.task_states(DAILY_BATCH_DAG, info["run_id"])
    except ApiError as exc:
        st.warning(f"Waiting for Airflow: {exc}")
        return
    state = run["state"]
    done = sum(t["state"] == "success" for t in tasks)
    st.markdown(
        f"Run `{info['run_id']}`: {RUN_ICONS.get(state, '')} **{state}** "
        f"({done}/{len(tasks) or 6} tasks done)"
    )
    before = info["before"]
    if state == "success":
        after = api_get(f"/api/v1/reports/daily/{info['day']}")["summary"]
        same = before["vehicle_count"] == after["vehicle_count"] and round(
            before["total_estimated_profit"], 2
        ) == round(after["total_estimated_profit"], 2)
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "": "before",
                        "vehicles": before["vehicle_count"],
                        "total profit": before["total_estimated_profit"],
                    },
                    {
                        "": "after",
                        "vehicles": after["vehicle_count"],
                        "total profit": after["total_estimated_profit"],
                    },
                ]
            ),
            hide_index=True,
        )
        if same:
            st.success("Identical result: the batch re-run is idempotent.")
        else:
            st.error("The report changed after the re-run.")
    elif state == "failed":
        st.error("The run failed; open the Airflow UI for the task logs.")


# ============================================================================ main
def main() -> None:
    st.set_page_config(page_title="Fleet Lambda", page_icon="🚕", layout="wide")
    st.sidebar.title("🚕 Fleet Lambda")
    page = st.sidebar.radio("Page", PAGES, label_visibility="collapsed")
    st.sidebar.divider()
    st.sidebar.caption(
        f"{CITY_NAME}, Sri Lanka · {CURRENCY} · Sri Lanka time\n\n"
        f"Fleet of {settings.fleet_size} vehicles · 1 simulated day = "
        f"{settings.sim_day_real_seconds / 60:.0f} real minutes"
    )
    {
        "Overview": page_overview,
        "Live Fleet": page_live_fleet,
        "Vehicles": page_vehicles,
        "Daily Report": page_daily_report,
        "Alerts": page_alerts,
        "Pipeline & Quality": page_pipeline,
    }[page]()


main()
