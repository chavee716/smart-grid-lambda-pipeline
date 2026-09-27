"""
Central configuration for the Smart Grid Lambda Pipeline.
All services read settings from environment variables (12-factor style),
with sane local-dev defaults so the stack works out-of-the-box with
docker-compose.
"""
import os


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_METER_READINGS = _env("TOPIC_METER_READINGS", "meter-readings")
TOPIC_ALERTS = _env("TOPIC_ALERTS", "grid-alerts")
KAFKA_PARTITIONS = int(_env("KAFKA_PARTITIONS", "6"))
KAFKA_REPLICATION_FACTOR = int(_env("KAFKA_REPLICATION_FACTOR", "1"))

# ---------------------------------------------------------------------------
# Postgres (serving + batch reconciliation store)
# ---------------------------------------------------------------------------
POSTGRES_HOST = _env("POSTGRES_HOST", "postgres")
POSTGRES_PORT = _env("POSTGRES_PORT", "5432")
POSTGRES_DB = _env("POSTGRES_DB", "smartgrid")
POSTGRES_USER = _env("POSTGRES_USER", "grid_user")
POSTGRES_PASSWORD = _env("POSTGRES_PASSWORD", "grid_pass")

POSTGRES_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ---------------------------------------------------------------------------
# Data lake (batch layer master dataset - "cold path")
# ---------------------------------------------------------------------------
# In production this would be S3/HDFS. For local demo we use a mounted
# volume with Parquet files partitioned by simulated date.
DATA_LAKE_ROOT = _env("DATA_LAKE_ROOT", "/data/lake")
RAW_READINGS_PATH = os.path.join(DATA_LAKE_ROOT, "raw_readings")
BATCH_DROPZONE_PATH = _env("BATCH_DROPZONE_PATH", "/data/batch_dropzone")

# ---------------------------------------------------------------------------
# Simulated clock
# ---------------------------------------------------------------------------
# 1 simulated day = SIMULATED_DAY_SECONDS real seconds. Default: 5 minutes.
SIMULATED_DAY_SECONDS = int(_env("SIMULATED_DAY_SECONDS", "300"))
METER_EMIT_INTERVAL_SECONDS = float(_env("METER_EMIT_INTERVAL_SECONDS", "2"))

# ---------------------------------------------------------------------------
# Grid topology used by simulators (household -> meter -> zone)
# ---------------------------------------------------------------------------
NUM_HOUSEHOLDS = int(_env("NUM_HOUSEHOLDS", "40"))
ZONES = ["North", "East", "South", "West", "Central"]

# ---------------------------------------------------------------------------
# Alert thresholds (observability)
# ---------------------------------------------------------------------------
LOW_RENEWABLE_SHARE_THRESHOLD = float(_env("LOW_RENEWABLE_SHARE_THRESHOLD", "0.15"))
NO_DATA_ALERT_MINUTES = int(_env("NO_DATA_ALERT_MINUTES", "2"))
ERROR_RATE_ALERT_THRESHOLD = float(_env("ERROR_RATE_ALERT_THRESHOLD", "0.05"))

# ---------------------------------------------------------------------------
# Serving API
# ---------------------------------------------------------------------------
API_HOST = _env("API_HOST", "0.0.0.0")
API_PORT = int(_env("API_PORT", "8000"))
