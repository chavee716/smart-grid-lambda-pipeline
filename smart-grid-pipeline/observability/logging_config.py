"""
Structured (JSON) logging used uniformly across ingestion, processing,
storage and serving stages so logs can be shipped to any log aggregator
(ELK, Loki, CloudWatch, ...) and correlated by `stage` and `component`.

Usage:
    from observability.logging_config import get_logger
    log = get_logger("stream_processing.spark_job")
    log.info("window_computed", extra={"stage": "speed_layer", "zone": "North", "rows": 120})
"""
import json
import logging
import sys
import time
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Renders each log record as a single-line JSON object."""

    RESERVED = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Merge in any `extra={...}` fields (component, stage, zone, latency_ms, etc.)
        for key, value in record.__dict__.items():
            if key not in self.RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(component: str) -> logging.Logger:
    logger = logging.getLogger(component)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class Timer:
    """Context manager to log stage latency, used for basic tracing."""

    def __init__(self, logger: logging.Logger, operation: str, **extra_fields):
        self.logger = logger
        self.operation = operation
        self.extra_fields = extra_fields

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        elapsed_ms = (time.perf_counter() - self._start) * 1000
        status = "error" if exc_type else "ok"
        self.logger.info(
            f"{self.operation} finished",
            extra={"operation": self.operation, "duration_ms": round(elapsed_ms, 2),
                   "status": status, **self.extra_fields},
        )
