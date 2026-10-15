"""Detector logic, run on small static DataFrames (same code path as the stream)."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from fraudstream import detectors as D

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
CITY = {"Boston": (42.36, -71.06), "Seattle": (47.61, -122.33), "Cambridge": (42.37, -71.11)}


def txn(i, card="C1", sec=0, amount=10.0, merchant=None, city="Boston"):
    lat, lon = CITY[city]
    return {"txn_id": f"t{i}", "card_id": card, "event_time": (T0 + timedelta(seconds=sec)).isoformat().replace("+00:00", "Z"),
            "amount": amount, "merchant_id": merchant or f"M{i}", "category": "x", "city": city,
            "lat": lat, "lon": lon, "channel": "online", "sent_at": T0.isoformat().replace("+00:00", "Z")}


def frame(spark, rows):
    raw = spark.createDataFrame([(json.dumps(r),) for r in rows], ["value"])
    return D.deduplicate(D.parse(raw))


def cards(df):
    return sorted({r["card_id"] for r in df.collect()})


# ---------------------------------------------------------------- parsing and duplicates
def test_bad_records_are_dropped(spark):
    rows = [txn(1), {**txn(2), "event_time": None}, {"not": "a transaction"}]
    raw = spark.createDataFrame([(json.dumps(r),) for r in rows] + [("{broken json",)], ["value"])
    assert D.parse(raw).count() == 1


def test_duplicate_deliveries_are_removed(spark):
    assert frame(spark, [txn(1), txn(1), txn(1), txn(2)]).count() == 2


# ---------------------------------------------------------------- card testing
def test_card_testing_flags_burst_of_tiny_charges(spark):
    rows = [txn(i, sec=i * 10, amount=1.5) for i in range(6)]
    assert cards(D.card_testing(frame(spark, rows))) == ["C1"]


def test_card_testing_ignores_normal_small_purchases(spark):
    # four transit taps in three minutes: under the threshold of five
    rows = [txn(i, sec=i * 40, amount=2.75) for i in range(4)]
    assert D.card_testing(frame(spark, rows)).count() == 0


def test_card_testing_ignores_burst_of_normal_amounts(spark):
    rows = [txn(i, sec=i * 10, amount=45.0) for i in range(8)]
    assert D.card_testing(frame(spark, rows)).count() == 0


# ---------------------------------------------------------------- account takeover
def test_takeover_flags_spree_across_many_merchants(spark):
    rows = [txn(i, sec=i * 20, amount=150.0) for i in range(7)]
    assert cards(D.account_takeover(frame(spark, rows))) == ["C1"]


def test_takeover_ignores_repeat_purchases_at_one_merchant(spark):
    rows = [txn(i, sec=i * 20, amount=150.0, merchant="M1") for i in range(7)]
    assert D.account_takeover(frame(spark, rows)).count() == 0


def test_takeover_ignores_small_basket_spree(spark):
    rows = [txn(i, sec=i * 20, amount=30.0) for i in range(7)]  # 7 merchants but only $210
    assert D.account_takeover(frame(spark, rows)).count() == 0


# ---------------------------------------------------------------- impossible travel
def test_travel_flags_cross_country_in_minutes(spark):
    rows = [txn(1, sec=0, city="Boston"), txn(2, sec=20 * 60, city="Seattle")]
    out = D.impossible_travel(frame(spark, rows)).collect()
    assert len(out) == 1 and out[0]["card_id"] == "C1"
    assert "Boston -> Seattle" in out[0]["evidence"]


def test_travel_ignores_nearby_cities(spark):
    rows = [txn(1, sec=0, city="Boston"), txn(2, sec=5 * 60, city="Cambridge")]
    assert D.impossible_travel(frame(spark, rows)).count() == 0


def test_travel_ignores_plausible_flight(spark):
    rows = [txn(1, sec=0, city="Boston"), txn(2, sec=7 * 3600, city="Seattle")]  # outside the 1 h lookback
    assert D.impossible_travel(frame(spark, rows)).count() == 0


def test_travel_never_pairs_different_cards(spark):
    rows = [txn(1, card="C1", city="Boston"), txn(2, card="C2", sec=60, city="Seattle")]
    assert D.impossible_travel(frame(spark, rows)).count() == 0


@pytest.mark.parametrize("a,b,expected", [("Boston", "Seattle", 3995), ("Boston", "Cambridge", 4.3)])
def test_haversine_matches_known_distances(a, b, expected):
    km = D.haversine_km(*CITY[a], *CITY[b])
    assert abs(km - expected) / expected < 0.05
