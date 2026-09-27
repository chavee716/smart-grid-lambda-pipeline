"""
Lightweight Prometheus metrics exposed by the serving API at /metrics.
Each pipeline stage imports the counters/gauges relevant to it.
"""
from prometheus_client import Counter, Gauge, Histogram

# --- Ingestion (speed layer, streaming source) -----------------------------
METER_EVENTS_PRODUCED = Counter(
    "meter_events_produced_total", "Smart-meter events published to Kafka", ["zone"]
)
METER_EVENTS_CONSUMED = Counter(
    "meter_events_consumed_total", "Smart-meter events consumed by the Spark streaming job", ["zone"]
)
INGESTION_ERRORS = Counter(
    "ingestion_errors_total", "Malformed or failed ingestion events", ["stage"]
)
LAST_EVENT_TIMESTAMP = Gauge(
    "last_event_timestamp_seconds", "Unix timestamp of the most recently processed streaming event"
)

# --- Stream processing (speed layer) ---------------------------------------
WINDOW_PROCESSING_LATENCY = Histogram(
    "window_processing_latency_seconds", "Time to compute one micro-batch window"
)
CURRENT_GRID_LOAD_KWH = Gauge(
    "current_grid_load_kwh", "Most recent windowed grid load per zone", ["zone"]
)
CURRENT_RENEWABLE_SHARE = Gauge(
    "current_renewable_share_ratio", "Most recent windowed renewable contribution ratio per zone", ["zone"]
)

# --- Batch layer -------------------------------------------------------------
BATCH_JOB_RUNS = Counter(
    "batch_job_runs_total", "Airflow-triggered batch job executions", ["dag", "status"]
)
BATCH_JOB_DURATION = Histogram(
    "batch_job_duration_seconds", "Duration of the daily billing reconciliation batch job"
)
HOUSEHOLDS_BILLED = Gauge(
    "households_billed_total", "Number of households included in the latest daily billing run"
)

# --- Alerting / health -------------------------------------------------------
ALERTS_RAISED = Counter(
    "alerts_raised_total", "Alerts raised by the health-check/alerting rules", ["rule"]
)
PIPELINE_HEALTHY = Gauge(
    "pipeline_healthy", "1 if the pipeline passed its last health check, else 0"
)
