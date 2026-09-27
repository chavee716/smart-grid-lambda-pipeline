# Smart Grid Energy Monitoring & Billing — Lambda Architecture Pipeline

EC8203 Applied Big Data Engineering — Data Engineering Mini-Project
**Use Case 3: Smart Grid Energy Monitoring & Billing**

An end-to-end **Lambda architecture** data platform that gives a utility company
(1) real-time visibility into grid load and renewable (solar) contribution by
zone, and (2) an authoritative daily household bill that reconciles the day's
consumption against tariff data delivered once a day. See `report/` for the
full architectural justification, diagrams, and results.

## Architecture at a glance

```
                         ┌────────────────────────────┐
 Smart meters (2s)  ───► │   Kafka: meter-readings     │
                         └──────────────┬─────────────┘
                                        │
                    ┌───────────────────┴────────────────────┐
                    ▼                                          ▼
        SPEED LAYER (Spark Structured Streaming)     Immutable raw archive
        1-min tumbling windows by zone                (Parquet, partitioned
        → realtime_grid_metrics (Postgres)             by simulated date)
                    │                                          │
                    ▼                                          ▼
            /realtime/* API                          BATCH LAYER (nightly,
                                                       orchestrated by Airflow)
 Tariff/weather file  ───► BATCH_DROPZONE  ──────────►  re-reads full raw day
 (once/simulated day)      (FileSensor)                 + tariff CSV, joins,
                                                         computes bill
                                                                │
                                                                ▼
                                                   daily_billing_report (Postgres)
                                                                │
                                                                ▼
                                                       /billing/* API

  Observability: structured JSON logs at every stage · Prometheus metrics
  (/metrics) · healthcheck-service polls Postgres every 30s and raises
  alerts (no-data / high-error-rate / low-renewable-share) to alert_log +
  Kafka `grid-alerts` topic.
```

**Why Lambda, not Kappa (short version — full argument in the report):**
the use case has two genuinely different correctness/latency requirements:
an approximate, fast dashboard view (grid load, renewable mix — a few
seconds of staleness is fine) and an authoritative, must-be-correct nightly
bill that can only be finalized once the late-arriving tariff file lands.
Kappa's single stream-replay model doesn't cleanly express "wait for a
daily reference dataset before finalizing", whereas Lambda's separate
speed/batch paths map directly onto the two SLAs.

## Repository layout

```
common/                 shared config (env-driven)
observability/          structured logging, Prometheus metrics, healthcheck/alerting
simulators/             streaming (smart meters) + daily-batch (tariff/weather) sources
stream_processing/      Spark Structured Streaming job (speed layer)
batch_processing/       billing reconciliation job (batch layer) + Airflow DAG
serving_api/            FastAPI app (speed-layer + batch-layer endpoints)
sql/init.sql            Postgres schema
tests/                  unit tests for core billing logic
docker-compose.yml      full stack orchestration
report/                 written report (PDF) + architecture diagrams
```

## Running the full stack

Requirements: Docker + Docker Compose (~4 GB RAM free is comfortable).

```bash
docker compose up --build
```

This brings up, in dependency order: Zookeeper, Kafka, the domain Postgres
(schema auto-applied from `sql/init.sql`), the Airflow metadata Postgres,
Airflow webserver + scheduler (DAG auto-mounted), the Spark Structured
Streaming job, the two simulators, the FastAPI serving layer, and the
health-check/alerting service.

| Service | URL |
|---|---|
| Serving API (docs) | http://localhost:8000/docs |
| Real-time grid load | http://localhost:8000/realtime/grid-load |
| Daily billing report | http://localhost:8000/billing/daily |
| Recent alerts | http://localhost:8000/alerts |
| Prometheus metrics | http://localhost:8000/metrics |
| Airflow UI | http://localhost:8080 (admin/admin) |

**Simulated clock:** by default, 1 simulated "day" = 300 seconds (5 minutes),
set via `SIMULATED_DAY_SECONDS` in `docker-compose.yml` / `.env`. The Airflow
DAG's schedule interval is tied to the same constant, and the batch
simulator drops one tariff CSV per simulated day into a shared Docker
volume (`batch_dropzone`) that both the simulator and Airflow mount.

To watch a full day/night + billing cycle end-to-end, let the stack run for
at least one `SIMULATED_DAY_SECONDS` interval, then check the Airflow UI for
a successful `daily_billing_reconciliation` DAG run and query
`/billing/daily`.

## Running components individually (no Docker)

Useful for development/debugging one piece at a time.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # edit hostnames to localhost

# Terminal 1: streaming source (requires a local Kafka broker)
python simulators/smart_meter_streamer.py --households 40 --interval 2

# Terminal 2: daily batch source
python simulators/daily_batch_generator.py --day-length 300

# Terminal 3: speed layer (requires local Spark + the kafka connector package)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    stream_processing/spark_streaming_job.py

# Terminal 4: serving API
uvicorn serving_api.main:app --reload

# Terminal 5: observability
python observability/healthcheck.py

# Ad-hoc / backfill a specific day's billing (no Airflow needed):
python batch_processing/daily_billing_job.py --date 2026-09-13
```

## Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

`tests/test_daily_billing_job.py` exercises the core batch-layer
transformation (aggregation, tariff join, subsidy logic, edge cases like a
household with solar exceeding consumption) without requiring Kafka/Spark/
Postgres to be running.

## Assumptions & simplifications (see report §7 for full discussion)

- Simulated day compressed to 5 minutes by default (configurable).
- Single-node Kafka/Spark for demo purposes; report discusses production
  scaling (replication factor, multi-broker, cluster-mode Spark).
- Billing model is simplified (flat subsidy %, no tiered/progressive rates)
  — deliberately kept simple so the join/reconciliation logic, which is
  what the assignment is assessing, stays legible.
- Weather-forecast feed is generated but only the tariff/reference join is
  scored on; it is included in the schema for extensibility (see report
  limitations).

## Contributions

- Completed individually by EG/2021/4479 Dias C.
