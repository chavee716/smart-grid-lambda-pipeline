"""
Serving layer: FastAPI application.

Merges the two Lambda-architecture views for API consumers:
  * /realtime/*  reads from the SPEED layer table (realtime_grid_metrics) -
                 fresh (seconds old) but approximate/re-computable.
  * /billing/*   reads from the BATCH layer table (daily_billing_report) -
                 authoritative, recomputed nightly.
  * /alerts      reads recent alerts raised by the observability service.
  * /metrics     exposes Prometheus metrics for scraping.
  * /healthz     liveness/readiness probe used by docker-compose / k8s.

This split is intentional and visible in the API surface: it makes the
Lambda architecture's "two paths, one merged view" explicit rather than
hiding it behind a single ambiguous endpoint.
"""
from datetime import date, datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from common import config
from observability.logging_config import get_logger

log = get_logger("serving_api.main")

app = FastAPI(
    title="Smart Grid Monitoring & Billing API",
    description="Serving layer for the Lambda-architecture smart grid pipeline",
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def get_conn():
    return psycopg2.connect(config.POSTGRES_URL, cursor_factory=psycopg2.extras.RealDictCursor)


@app.get("/healthz")
def healthz():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"database unreachable: {e}")


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# SPEED LAYER endpoints (real-time grid load / renewable mix)
# ---------------------------------------------------------------------------
@app.get("/realtime/grid-load")
def realtime_grid_load(zone: Optional[str] = Query(None, description="Filter to a single zone")):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if zone:
                cur.execute("SELECT * FROM v_current_grid_snapshot WHERE zone = %s;", (zone,))
            else:
                cur.execute("SELECT * FROM v_current_grid_snapshot ORDER BY zone;")
            rows = cur.fetchall()
        if zone and not rows:
            raise HTTPException(status_code=404, detail=f"No data for zone '{zone}'")
        return {"as_of": datetime.utcnow().isoformat(), "zones": rows}
    finally:
        conn.close()


@app.get("/realtime/renewable-mix")
def realtime_renewable_mix():
    """System-wide renewable contribution, aggregated across all zones' latest windows."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    SUM(total_consumption_kwh) AS total_consumption_kwh,
                    SUM(total_solar_kwh) AS total_solar_kwh,
                    CASE WHEN SUM(total_consumption_kwh) > 0
                         THEN SUM(total_solar_kwh) / SUM(total_consumption_kwh)
                         ELSE 0 END AS system_renewable_share
                FROM v_current_grid_snapshot;
            """)
            row = cur.fetchone()
        return {"as_of": datetime.utcnow().isoformat(), **row}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# BATCH LAYER endpoints (authoritative daily billing)
# ---------------------------------------------------------------------------
@app.get("/billing/daily/{household_id}")
def billing_for_household(household_id: str, billing_date: Optional[date] = None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if billing_date:
                cur.execute(
                    "SELECT * FROM daily_billing_report WHERE household_id = %s AND billing_date = %s;",
                    (household_id, billing_date),
                )
            else:
                cur.execute(
                    "SELECT * FROM daily_billing_report WHERE household_id = %s "
                    "ORDER BY billing_date DESC LIMIT 1;",
                    (household_id,),
                )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No billing record found")
        return row
    finally:
        conn.close()


@app.get("/billing/daily")
def billing_report(billing_date: Optional[date] = None, limit: int = 100):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if billing_date:
                cur.execute(
                    "SELECT * FROM daily_billing_report WHERE billing_date = %s "
                    "ORDER BY household_id LIMIT %s;", (billing_date, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM daily_billing_report "
                    "ORDER BY billing_date DESC, household_id LIMIT %s;", (limit,),
                )
            rows = cur.fetchall()
        return {"count": len(rows), "households": rows}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# OBSERVABILITY endpoints
# ---------------------------------------------------------------------------
@app.get("/alerts")
def recent_alerts(limit: int = 20):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM alert_log ORDER BY raised_at DESC LIMIT %s;", (limit,))
            rows = cur.fetchall()
        return {"count": len(rows), "alerts": rows}
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)
