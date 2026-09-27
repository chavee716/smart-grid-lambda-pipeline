"""
Standalone health-check / alerting service.

Implements the two minimum observability rules required by the brief:
  1. "No data received in N minutes"       -> checks last event timestamp
  2. "Error rate above threshold"          -> checks ingestion error counters
Plus a use-case-specific rule:
  3. "Renewable contribution considerably low" for any zone.

Run as its own long-lived container (see docker-compose.yml), polling
Postgres every 30s. Alerts are logged (structured) and published to the
`grid-alerts` Kafka topic so any downstream system (Slack webhook, PagerDuty,
email) can subscribe without coupling to this service.

This is intentionally simple (poll + threshold) rather than a full
Prometheus Alertmanager setup, to keep the demo self-contained while still
satisfying "at least one basic alert / health-check rule".
"""
import json
import time
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaProducer

from common import config
from observability.logging_config import get_logger
from observability.metrics import ALERTS_RAISED, PIPELINE_HEALTHY

log = get_logger("observability.healthcheck")

POLL_INTERVAL_SECONDS = 30


def get_connection():
    return psycopg2.connect(config.POSTGRES_URL)


def get_producer():
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def check_no_data(conn) -> list[dict]:
    """Rule 1: alert if no meter reading has landed in the real-time table recently."""
    alerts = []
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(window_end) FROM realtime_grid_metrics;")
        row = cur.fetchone()
        last_ts = row[0]
    if last_ts is None:
        alerts.append({"rule": "no_data_received", "detail": "No streaming data has ever landed."})
        return alerts
    age_minutes = (datetime.now(timezone.utc) - last_ts.replace(tzinfo=timezone.utc)).total_seconds() / 60
    if age_minutes > config.NO_DATA_ALERT_MINUTES:
        alerts.append({
            "rule": "no_data_received",
            "detail": f"No new windowed metrics in {age_minutes:.1f} minutes "
                      f"(threshold={config.NO_DATA_ALERT_MINUTES}m).",
        })
    return alerts


def check_low_renewable_share(conn) -> list[dict]:
    """Rule 3 (use-case specific): alert per zone if renewable share is too low."""
    alerts = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (zone) zone, renewable_share, window_end
            FROM realtime_grid_metrics
            ORDER BY zone, window_end DESC;
        """)
        rows = cur.fetchall()
    for zone, renewable_share, window_end in rows:
        if renewable_share is not None and renewable_share < config.LOW_RENEWABLE_SHARE_THRESHOLD:
            alerts.append({
                "rule": "low_renewable_share",
                "zone": zone,
                "detail": f"Zone {zone} renewable share {renewable_share:.2%} "
                          f"below threshold {config.LOW_RENEWABLE_SHARE_THRESHOLD:.0%}.",
            })
    return alerts


def check_error_rate(conn) -> list[dict]:
    """Rule 2: alert if ingestion error rate over the last window is too high."""
    alerts = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(SUM(malformed_count), 0), COALESCE(SUM(total_count), 0)
            FROM ingestion_quality_stats
            WHERE recorded_at > NOW() - INTERVAL '5 minutes';
        """)
        malformed, total = cur.fetchone()
    if total and (malformed / total) > config.ERROR_RATE_ALERT_THRESHOLD:
        alerts.append({
            "rule": "high_error_rate",
            "detail": f"Ingestion error rate {malformed/total:.2%} over last 5 min "
                      f"exceeds threshold {config.ERROR_RATE_ALERT_THRESHOLD:.0%}.",
        })
    return alerts


def run_once(conn, producer) -> bool:
    all_alerts = []
    for check in (check_no_data, check_low_renewable_share, check_error_rate):
        try:
            all_alerts.extend(check(conn))
        except Exception as e:  # a check itself failing is unhealthy too
            log.error("healthcheck rule failed", extra={"rule": check.__name__, "error": str(e)})
            all_alerts.append({"rule": check.__name__, "detail": f"check raised exception: {e}"})

    for alert in all_alerts:
        ALERTS_RAISED.labels(rule=alert["rule"]).inc()
        log.warning("alert_raised", extra={"stage": "observability", **alert})
        producer.send(config.TOPIC_ALERTS, {**alert, "raised_at": datetime.now(timezone.utc).isoformat()})
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO alert_log (rule, zone, detail) VALUES (%s, %s, %s);",
                (alert["rule"], alert.get("zone"), alert["detail"]),
            )
        conn.commit()

    healthy = len(all_alerts) == 0
    PIPELINE_HEALTHY.set(1 if healthy else 0)
    return healthy


def main():
    conn = get_connection()
    producer = get_producer()
    log.info("healthcheck_service_started", extra={"stage": "observability",
                                                     "poll_interval_s": POLL_INTERVAL_SECONDS})
    while True:
        healthy = run_once(conn, producer)
        log.info("healthcheck_cycle_complete", extra={"stage": "observability", "healthy": healthy})
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
