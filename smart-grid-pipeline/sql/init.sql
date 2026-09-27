-- ===========================================================================
-- Smart Grid Lambda Pipeline - Serving & Reconciliation Store (PostgreSQL)
-- ===========================================================================
-- This schema backs BOTH layers of the Lambda architecture's serving layer:
--   * realtime_grid_metrics      <- written by the SPEED layer (Spark Structured
--                                    Streaming), overwritten every micro-batch
--   * daily_billing_report       <- written by the BATCH layer (Airflow DAG),
--                                    the source of truth, recomputed nightly
--   * tariff_reference           <- daily-batch reference data (dimension table)
--   * ingestion_quality_stats    <- observability: error/malformed counts
--   * alert_log                  <- observability: alert history
-- The two report tables are deliberately separate (per the Lambda pattern):
-- the speed-layer table is fast-but-approximate and can be blown away and
-- rebuilt at any time; the batch-layer table is authoritative.

CREATE TABLE IF NOT EXISTS tariff_reference (
    household_id     TEXT NOT NULL,
    effective_date    DATE NOT NULL,
    tariff_rate       NUMERIC(10, 4) NOT NULL,   -- $ per kWh
    billing_tier      TEXT NOT NULL,
    subsidy_flag      BOOLEAN NOT NULL DEFAULT FALSE,
    forecast_solar_irradiance NUMERIC(6, 3),      -- from weather forecast feed
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (household_id, effective_date)
);

-- Speed layer: latest windowed (e.g. 1-minute tumbling window) grid metrics
-- by zone. Continuously overwritten/upserted by the Spark streaming job.
CREATE TABLE IF NOT EXISTS realtime_grid_metrics (
    zone                TEXT NOT NULL,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    total_consumption_kwh   NUMERIC(12, 3) NOT NULL,
    total_solar_kwh         NUMERIC(12, 3) NOT NULL,
    renewable_share          NUMERIC(6, 4) NOT NULL,     -- solar / (consumption)
    active_meters            INTEGER NOT NULL,
    avg_load_kwh             NUMERIC(12, 3) NOT NULL,
    PRIMARY KEY (zone, window_start)
);
CREATE INDEX IF NOT EXISTS idx_realtime_metrics_window
    ON realtime_grid_metrics (window_end DESC);

-- Batch layer: authoritative, nightly-reconciled per-household bill,
-- joining the day's raw consumption (recomputed from the immutable
-- data-lake log, NOT the speed-layer table) against tariff_reference.
CREATE TABLE IF NOT EXISTS daily_billing_report (
    household_id        TEXT NOT NULL,
    billing_date          DATE NOT NULL,
    total_consumption_kwh NUMERIC(12, 3) NOT NULL,
    total_solar_kwh        NUMERIC(12, 3) NOT NULL,
    net_grid_kwh            NUMERIC(12, 3) NOT NULL,  -- consumption - solar, floored at 0
    tariff_rate              NUMERIC(10, 4) NOT NULL,
    subsidy_flag             BOOLEAN NOT NULL,
    bill_amount               NUMERIC(12, 2) NOT NULL,
    solar_contribution_pct    NUMERIC(6, 4) NOT NULL,
    computed_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (household_id, billing_date)
);

-- Observability: rolling counts of well-formed vs malformed events, recorded
-- per short interval by the stream-processing job.
CREATE TABLE IF NOT EXISTS ingestion_quality_stats (
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_count     INTEGER NOT NULL,
    malformed_count INTEGER NOT NULL
);

-- Observability: durable alert history (mirrors what's published to Kafka).
CREATE TABLE IF NOT EXISTS alert_log (
    id          SERIAL PRIMARY KEY,
    rule        TEXT NOT NULL,
    zone        TEXT,
    detail      TEXT NOT NULL,
    raised_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convenience view for the "current" dashboard snapshot (latest window per zone)
CREATE OR REPLACE VIEW v_current_grid_snapshot AS
SELECT DISTINCT ON (zone) zone, window_start, window_end,
       total_consumption_kwh, total_solar_kwh, renewable_share,
       active_meters, avg_load_kwh
FROM realtime_grid_metrics
ORDER BY zone, window_end DESC;
