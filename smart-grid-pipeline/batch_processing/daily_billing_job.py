"""
Batch layer: daily billing & solar-contribution reconciliation job.

This is the authoritative recomputation step of the Lambda architecture.
Unlike the speed layer (which only ever sees a bounded, watermarked window
of recent events), this job re-reads the FULL immutable raw-event archive
for the target billing date from the data lake (Parquet, written by the
Spark streaming job), and joins it against the tariff/weather file dropped
by the daily-batch simulator. It recomputes bills from scratch every run,
so it is naturally idempotent and safe to re-run/backfill.

Invoked by the Airflow DAG `daily_billing_dag.py`. Kept as a standalone
script (rather than only living inside the DAG file) so it can be unit
tested and re-run manually / backfilled for any date without Airflow.
"""
import argparse
import glob
import os
from datetime import date, datetime

import pandas as pd
import psycopg2

from common import config
from observability.logging_config import get_logger, Timer
from observability.metrics import BATCH_JOB_DURATION, HOUSEHOLDS_BILLED

log = get_logger("batch_processing.daily_billing_job")


def load_raw_consumption(billing_date: date) -> pd.DataFrame:
    """Reads the immutable Parquet archive for the given simulated date."""
    partition_glob = os.path.join(config.RAW_READINGS_PATH, f"event_date={billing_date.isoformat()}", "*.parquet")
    files = glob.glob(partition_glob)
    if not files:
        log.warning("no_raw_partition_found", extra={
            "stage": "batch_layer", "billing_date": billing_date.isoformat(), "glob": partition_glob,
        })
        return pd.DataFrame(columns=["household_id", "power_consumption_kwh", "solar_generation_kwh"])
    frames = [pd.read_parquet(f) for f in files]
    return pd.concat(frames, ignore_index=True)


def load_tariff_file(billing_date: date) -> pd.DataFrame:
    """Reads the CSV file dropped by the daily batch simulator for this date."""
    path = os.path.join(config.BATCH_DROPZONE_PATH, f"tariff_{billing_date.isoformat()}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Tariff batch file not found for {billing_date}: {path}")
    return pd.read_csv(path)


def compute_daily_bills(consumption_df: pd.DataFrame, tariff_df: pd.DataFrame, billing_date: date) -> pd.DataFrame:
    daily_totals = (
        consumption_df.groupby("household_id")
        .agg(
            total_consumption_kwh=("power_consumption_kwh", "sum"),
            total_solar_kwh=("solar_generation_kwh", "sum"),
        )
        .reset_index()
    )

    merged = daily_totals.merge(tariff_df, on="household_id", how="right")  # right join: bill every known household
    merged["total_consumption_kwh"] = merged["total_consumption_kwh"].fillna(0.0)
    merged["total_solar_kwh"] = merged["total_solar_kwh"].fillna(0.0)

    merged["net_grid_kwh"] = (merged["total_consumption_kwh"] - merged["total_solar_kwh"]).clip(lower=0)
    merged["bill_amount"] = merged["net_grid_kwh"] * merged["tariff_rate"]
    # Subsidy: flat 15% reduction for flagged households
    merged.loc[merged["subsidy_flag"].astype(bool), "bill_amount"] *= 0.85
    merged["bill_amount"] = merged["bill_amount"].round(2)

    merged["solar_contribution_pct"] = merged.apply(
        lambda r: (r["total_solar_kwh"] / r["total_consumption_kwh"]) if r["total_consumption_kwh"] > 0 else 0.0,
        axis=1,
    )
    merged["billing_date"] = billing_date

    return merged[[
        "household_id", "billing_date", "total_consumption_kwh", "total_solar_kwh",
        "net_grid_kwh", "tariff_rate", "subsidy_flag", "bill_amount", "solar_contribution_pct",
    ]]


def write_to_postgres(bills_df: pd.DataFrame, tariff_df: pd.DataFrame, billing_date: date):
    conn = psycopg2.connect(config.POSTGRES_URL)
    try:
        with conn.cursor() as cur:
            # Reference data (idempotent upsert)
            for _, r in tariff_df.iterrows():
                cur.execute("""
                    INSERT INTO tariff_reference
                        (household_id, effective_date, tariff_rate, billing_tier, subsidy_flag, forecast_solar_irradiance)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (household_id, effective_date) DO UPDATE SET
                        tariff_rate = EXCLUDED.tariff_rate,
                        billing_tier = EXCLUDED.billing_tier,
                        subsidy_flag = EXCLUDED.subsidy_flag,
                        forecast_solar_irradiance = EXCLUDED.forecast_solar_irradiance;
                """, (r["household_id"], r["effective_date"], r["tariff_rate"],
                      r["billing_tier"], bool(r["subsidy_flag"]), r.get("forecast_solar_irradiance")))

            # Idempotent: delete+insert for this billing_date makes reruns/backfills safe
            cur.execute("DELETE FROM daily_billing_report WHERE billing_date = %s;", (billing_date,))
            for _, r in bills_df.iterrows():
                cur.execute("""
                    INSERT INTO daily_billing_report
                        (household_id, billing_date, total_consumption_kwh, total_solar_kwh,
                         net_grid_kwh, tariff_rate, subsidy_flag, bill_amount, solar_contribution_pct)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (r["household_id"], r["billing_date"], r["total_consumption_kwh"], r["total_solar_kwh"],
                      r["net_grid_kwh"], r["tariff_rate"], bool(r["subsidy_flag"]), r["bill_amount"],
                      r["solar_contribution_pct"]))
        conn.commit()
    finally:
        conn.close()


def run(billing_date: date):
    with Timer(log, "daily_billing_job", billing_date=billing_date.isoformat()):
        with BATCH_JOB_DURATION.time():
            consumption_df = load_raw_consumption(billing_date)
            tariff_df = load_tariff_file(billing_date)
            bills_df = compute_daily_bills(consumption_df, tariff_df, billing_date)
            write_to_postgres(bills_df, tariff_df, billing_date)
            HOUSEHOLDS_BILLED.set(len(bills_df))
            log.info("daily_billing_complete", extra={
                "stage": "batch_layer", "billing_date": billing_date.isoformat(),
                "households_billed": len(bills_df),
                "total_billed_amount": round(float(bills_df["bill_amount"].sum()), 2),
            })


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=date.today().isoformat(),
                         help="Billing date to (re)compute, YYYY-MM-DD")
    args = parser.parse_args()
    run(datetime.strptime(args.date, "%Y-%m-%d").date())
