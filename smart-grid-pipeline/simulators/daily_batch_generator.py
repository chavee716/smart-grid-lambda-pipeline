"""
Daily-batch data source simulator: tariff/billing reference data and a
weather-forecast feed (affects expected solar output), dropped once per
simulated day as a CSV file into BATCH_DROPZONE_PATH.

This mirrors a realistic pattern where an external billing system or
utility-regulator feed lands a file on a shared drive / SFTP / S3 bucket
once a day. Airflow's DAG polls this dropzone (FileSensor) and ingests
whatever new file appears - decoupling the *arrival* of the file from the
*simulator's* internal clock, exactly as it would be decoupled from a real
third-party system.

Simulated day length is controlled by config.SIMULATED_DAY_SECONDS
(default 300s = 5 minutes), so a full day/night + billing cycle can be
demoed in a single session.
"""
import argparse
import csv
import os
import random
import time
from datetime import date, timedelta

from common import config
from observability.logging_config import get_logger

log = get_logger("simulators.daily_batch_generator")

BILLING_TIERS = ["residential_standard", "residential_low_income", "residential_premium"]


def generate_tariff_file(sim_day_index: int, out_dir: str) -> str:
    billing_date = date.today() + timedelta(days=sim_day_index)
    filename = f"tariff_{billing_date.isoformat()}.csv"
    filepath = os.path.join(out_dir, filename)

    os.makedirs(out_dir, exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "household_id", "effective_date", "tariff_rate", "billing_tier",
            "subsidy_flag", "forecast_solar_irradiance",
        ])
        # Simple diurnal-independent daily weather forecast affecting solar output
        forecast_irradiance = round(random.uniform(2.5, 6.5), 3)  # kWh/m^2/day equivalent
        for i in range(config.NUM_HOUSEHOLDS):
            household_id = f"H-{i:04d}"
            tier = random.choices(BILLING_TIERS, weights=[0.7, 0.2, 0.1])[0]
            base_rate = {
                "residential_standard": 0.28,
                "residential_low_income": 0.18,
                "residential_premium": 0.35,
            }[tier]
            tariff_rate = round(base_rate * random.uniform(0.95, 1.08), 4)
            subsidy = tier == "residential_low_income" and random.random() < 0.6
            writer.writerow([
                household_id, billing_date.isoformat(), tariff_rate, tier,
                subsidy, forecast_irradiance,
            ])

    log.info("batch_file_dropped", extra={
        "stage": "ingestion", "component": "daily_batch_generator",
        "file": filepath, "billing_date": billing_date.isoformat(),
        "rows": config.NUM_HOUSEHOLDS,
    })
    return filepath


def run(num_days: int | None, day_length_seconds: float, out_dir: str):
    day_index = 0
    while num_days is None or day_index < num_days:
        generate_tariff_file(day_index, out_dir)
        day_index += 1
        time.sleep(day_length_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily tariff/billing batch source simulator")
    parser.add_argument("--num-days", type=int, default=None, help="Number of days to simulate (default: infinite)")
    parser.add_argument("--day-length", type=float, default=config.SIMULATED_DAY_SECONDS)
    parser.add_argument("--out-dir", type=str, default=config.BATCH_DROPZONE_PATH)
    args = parser.parse_args()
    run(args.num_days, args.day_length, args.out_dir)
