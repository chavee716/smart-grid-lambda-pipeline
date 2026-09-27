"""
Unit tests for the batch layer's billing computation logic. These do NOT
require Kafka/Spark/Postgres to be running - they test the pure pandas
transformation in isolation, which is the "core pipeline logic" the brief
asks students to be able to defend in a viva.
"""
from datetime import date

import pandas as pd
import pytest

from batch_processing.daily_billing_job import compute_daily_bills


@pytest.fixture
def sample_consumption():
    return pd.DataFrame([
        {"household_id": "H-0001", "power_consumption_kwh": 3.0, "solar_generation_kwh": 1.0},
        {"household_id": "H-0001", "power_consumption_kwh": 2.0, "solar_generation_kwh": 0.5},
        {"household_id": "H-0002", "power_consumption_kwh": 5.0, "solar_generation_kwh": 0.0},
    ])


@pytest.fixture
def sample_tariff():
    return pd.DataFrame([
        {"household_id": "H-0001", "effective_date": "2026-09-13", "tariff_rate": 0.30,
         "billing_tier": "residential_standard", "subsidy_flag": False},
        {"household_id": "H-0002", "effective_date": "2026-09-13", "tariff_rate": 0.20,
         "billing_tier": "residential_low_income", "subsidy_flag": True},
        {"household_id": "H-0003", "effective_date": "2026-09-13", "tariff_rate": 0.25,
         "billing_tier": "residential_standard", "subsidy_flag": False},  # no meter data at all
    ])


def test_aggregates_multiple_readings_per_household(sample_consumption, sample_tariff):
    bills = compute_daily_bills(sample_consumption, sample_tariff, date(2026, 9, 13))
    h1 = bills[bills.household_id == "H-0001"].iloc[0]
    assert h1.total_consumption_kwh == pytest.approx(5.0)
    assert h1.total_solar_kwh == pytest.approx(1.5)


def test_bill_amount_uses_net_grid_consumption(sample_consumption, sample_tariff):
    bills = compute_daily_bills(sample_consumption, sample_tariff, date(2026, 9, 13))
    h1 = bills[bills.household_id == "H-0001"].iloc[0]
    # net_grid = 5.0 - 1.5 = 3.5 ; bill = 3.5 * 0.30 = 1.05
    assert h1.net_grid_kwh == pytest.approx(3.5)
    assert h1.bill_amount == pytest.approx(1.05)


def test_subsidy_reduces_bill_by_15_percent(sample_consumption, sample_tariff):
    bills = compute_daily_bills(sample_consumption, sample_tariff, date(2026, 9, 13))
    h2 = bills[bills.household_id == "H-0002"].iloc[0]
    # net_grid = 5.0 - 0 = 5.0 ; base bill = 5.0 * 0.20 = 1.0 ; subsidised = 0.85
    assert h2.bill_amount == pytest.approx(0.85)


def test_household_with_no_meter_data_still_billed_at_zero(sample_consumption, sample_tariff):
    bills = compute_daily_bills(sample_consumption, sample_tariff, date(2026, 9, 13))
    h3 = bills[bills.household_id == "H-0003"].iloc[0]
    assert h3.total_consumption_kwh == 0.0
    assert h3.bill_amount == 0.0


def test_net_grid_kwh_floored_at_zero_when_solar_exceeds_consumption():
    consumption = pd.DataFrame([
        {"household_id": "H-solar", "power_consumption_kwh": 1.0, "solar_generation_kwh": 3.0},
    ])
    tariff = pd.DataFrame([
        {"household_id": "H-solar", "effective_date": "2026-09-13", "tariff_rate": 0.30,
         "billing_tier": "residential_standard", "subsidy_flag": False},
    ])
    bills = compute_daily_bills(consumption, tariff, date(2026, 9, 13))
    row = bills.iloc[0]
    assert row.net_grid_kwh == 0.0
    assert row.bill_amount == 0.0
