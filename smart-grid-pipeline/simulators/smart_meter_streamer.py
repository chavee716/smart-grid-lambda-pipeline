"""
Streaming data source simulator: smart meters.

Emits one JSON event per household every METER_EMIT_INTERVAL_SECONDS to the
`meter-readings` Kafka topic:

    {
        "meter_id": "M-0007",
        "household_id": "H-0007",
        "power_consumption_kwh": 1.42,
        "solar_generation_kwh": 0.31,
        "grid_zone": "North",
        "timestamp": "2026-09-13T04:12:31.512Z"
    }

Design notes (see report for full justification):
  * Each household has a persistent baseline load + a solar profile that
    follows a simulated diurnal (day/night) curve tied to SIMULATED_DAY_SECONDS,
    so "solar generation" plausibly peaks at simulated midday and is ~0 at
    simulated night - this is what lets the low-renewable-share alert and the
    daily billing report tell a believable story.
  * A small, controllable fraction of events are intentionally malformed
    (missing field / bad type) to exercise the pipeline's data-quality
    handling and the "error rate" observability rule end-to-end.
  * Uses Kafka's key = household_id so all readings for a household land on
    the same partition, giving ordering per household without a global
    ordering guarantee (acceptable for this use case; see report trade-offs).
"""
import argparse
import json
import math
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from common import config
from observability.logging_config import get_logger
from observability.metrics import METER_EVENTS_PRODUCED

log = get_logger("simulators.smart_meter_streamer")

MALFORMED_EVENT_RATE = 0.02  # 2% intentionally bad events for pipeline testing


class Household:
    def __init__(self, idx: int):
        self.household_id = f"H-{idx:04d}"
        self.meter_id = f"M-{idx:04d}"
        self.zone = config.ZONES[idx % len(config.ZONES)]
        # Baseline consumption varies per household (kWh per emit interval)
        self.base_load = random.uniform(0.15, 0.9)
        # Solar panel capacity: ~30% of households have solar installed.
        # Sized relative to the household's own baseline load (typical rooftop
        # installs cover roughly 0.6x-1.3x daytime household draw) rather than
        # an independent random range, so system-wide renewable share stays
        # in a realistic band (a household CAN still be a net exporter at
        # solar noon, but the aggregate zone/system share won't blow past
        # ~100-150% the way an unbounded random capacity would).
        self.has_solar = random.random() < 0.30
        self.solar_capacity = self.base_load * random.uniform(0.6, 1.3) if self.has_solar else 0.0

    def simulated_time_of_day_fraction(self, sim_seconds_elapsed: float) -> float:
        """Returns 0.0-1.0 representing position within the simulated day."""
        return (sim_seconds_elapsed % config.SIMULATED_DAY_SECONDS) / config.SIMULATED_DAY_SECONDS

    def next_reading(self, sim_seconds_elapsed: float) -> dict:
        tod = self.simulated_time_of_day_fraction(sim_seconds_elapsed)  # 0=midnight .. 1=next midnight

        # Consumption: higher in simulated morning/evening (human activity), lower at night
        activity_curve = 0.6 + 0.4 * math.sin(2 * math.pi * (tod - 0.15))
        consumption = max(0.02, self.base_load * activity_curve * random.uniform(0.85, 1.15))

        # Solar: bell curve peaking at simulated noon (tod=0.5), zero at night
        daylight = max(0.0, math.sin(math.pi * tod))
        solar = round(self.solar_capacity * daylight * random.uniform(0.9, 1.1), 4) if self.has_solar else 0.0

        return {
            "meter_id": self.meter_id,
            "household_id": self.household_id,
            "power_consumption_kwh": round(consumption, 4),
            "solar_generation_kwh": round(solar, 4),
            "grid_zone": self.zone,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def maybe_corrupt(event: dict) -> dict:
    """Randomly corrupts an event to simulate real-world data quality issues."""
    if random.random() >= MALFORMED_EVENT_RATE:
        return event
    corruption = random.choice(["missing_field", "bad_type", "negative_value"])
    corrupted = dict(event)
    if corruption == "missing_field":
        corrupted.pop("grid_zone", None)
    elif corruption == "bad_type":
        corrupted["power_consumption_kwh"] = "N/A"
    elif corruption == "negative_value":
        corrupted["power_consumption_kwh"] = -abs(corrupted["power_consumption_kwh"])
    corrupted["_simulated_corruption"] = corruption
    return corrupted


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
    )


def run(num_households: int, interval_seconds: float, max_events: int | None = None):
    producer = build_producer()
    households = [Household(i) for i in range(num_households)]
    log.info("streamer_started", extra={
        "stage": "ingestion", "component": "smart_meter_streamer",
        "num_households": num_households, "interval_seconds": interval_seconds,
    })

    start_time = time.time()
    events_sent = 0
    try:
        while max_events is None or events_sent < max_events:
            sim_elapsed = time.time() - start_time
            for hh in households:
                event = hh.next_reading(sim_elapsed)
                event = maybe_corrupt(event)
                producer.send(config.TOPIC_METER_READINGS, key=hh.household_id, value=event)
                METER_EVENTS_PRODUCED.labels(zone=event.get("grid_zone", "unknown")).inc()
                events_sent += 1
            producer.flush()
            log.info("batch_emitted", extra={
                "stage": "ingestion", "component": "smart_meter_streamer",
                "events_this_tick": num_households, "total_events_sent": events_sent,
            })
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        log.info("streamer_stopped_by_user", extra={"stage": "ingestion"})
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart meter streaming source simulator")
    parser.add_argument("--households", type=int, default=config.NUM_HOUSEHOLDS)
    parser.add_argument("--interval", type=float, default=config.METER_EMIT_INTERVAL_SECONDS)
    parser.add_argument("--max-events", type=int, default=None, help="Stop after N total events (testing)")
    args = parser.parse_args()
    run(args.households, args.interval, args.max_events)
