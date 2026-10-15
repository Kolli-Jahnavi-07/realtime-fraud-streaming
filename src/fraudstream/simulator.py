"""Card-transaction simulator with hidden fraud attacks.

Emits a realistic stream of card authorizations for a population of cardholders,
then mixes in three attack patterns that the streaming job has to find on its own:

* card_testing      a stolen card is probed with a burst of tiny charges (< $5)
* impossible_travel the same card is used in two cities too far apart to travel between
* account_takeover  a burst of purchases at many different merchants in a few minutes

To keep the problem honest, the normal traffic includes look-alikes: small transit and
parking charges, and legitimate shopping sprees (several new merchants in a few
minutes at a mall). These are labeled ``benign_spree`` in the answer key so false
positives can be measured instead of hidden.

The stream also has the defects real payment feeds have: duplicate deliveries
(network retries) and events that arrive late and out of order.

The answer key (which transactions belong to which attack) is written separately and
never reaches the streaming job; it is only used afterwards to score precision/recall.

Sinks: ``files`` writes JSON micro-batch files (local runs and tests), ``kafka``
publishes to a Kafka topic (docker-compose and CI). All data is synthetic.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

CITIES = {
    "New York": (40.71, -74.01), "Boston": (42.36, -71.06), "Chicago": (41.88, -87.63),
    "Dallas": (32.78, -96.80), "Denver": (39.74, -104.99), "Seattle": (47.61, -122.33),
    "San Francisco": (37.77, -122.42), "Los Angeles": (34.05, -118.24), "Miami": (25.76, -80.19),
    "Atlanta": (33.75, -84.39), "Cincinnati": (39.10, -84.51), "Phoenix": (33.45, -112.07),
}
CATEGORIES = {"grocery": (8, 140), "fuel": (20, 90), "restaurant": (12, 120), "online_retail": (10, 400),
              "electronics": (60, 1500), "travel": (90, 900), "pharmacy": (6, 80), "subscription": (5, 30),
              "transit": (1.5, 4.75), "parking": (2, 4.9)}
CATEGORY_WEIGHTS = {"grocery": 18, "fuel": 12, "restaurant": 18, "online_retail": 16, "electronics": 4,
                    "travel": 3, "pharmacy": 7, "subscription": 6, "transit": 9, "parking": 7}


def km(a: tuple[float, float], b: tuple[float, float]) -> float:
    (la1, lo1), (la2, lo2) = map(lambda p: (math.radians(p[0]), math.radians(p[1])), (a, b))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(h))


@dataclass
class Card:
    card_id: str
    home: str
    merchants: list[str]


class Simulator:
    def __init__(self, seed: int = 7, cards: int = 20_000, start: datetime | None = None):
        self.rng = random.Random(seed)
        self.t = start or datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
        self.cards = []
        for i in range(cards):
            home = self.rng.choice(list(CITIES))
            merchants = [f"M{self.rng.randint(1000, 9999)}" for _ in range(self.rng.randint(4, 9))]
            self.cards.append(Card(f"C{100000 + i}", home, merchants))
        self.truth: list[dict] = []

    def _txn(self, card: Card, ts: datetime, city: str, amount: float, merchant: str, category: str) -> dict:
        lat, lon = CITIES[city]
        return {
            "txn_id": uuid.UUID(int=self.rng.getrandbits(128)).hex,
            "card_id": card.card_id,
            "event_time": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "amount": round(amount, 2),
            "merchant_id": merchant,
            "category": category,
            "city": city,
            "lat": lat + self.rng.uniform(-0.05, 0.05),
            "lon": lon + self.rng.uniform(-0.05, 0.05),
            "channel": self.rng.choices(["chip", "contactless", "online"], [0.45, 0.3, 0.25])[0],
        }

    def normal(self, ts: datetime) -> dict:
        card = self.rng.choice(self.cards)
        cat = self.rng.choices(list(CATEGORY_WEIGHTS), list(CATEGORY_WEIGHTS.values()))[0]
        lo, hi = CATEGORIES[cat]
        return self._txn(card, ts, card.home, self.rng.uniform(lo, hi), self.rng.choice(card.merchants), cat)

    def attack(self, kind: str, ts: datetime) -> list[dict]:
        card = self.rng.choice(self.cards)
        attack_id = f"A{len(self.truth) + 1:04d}"
        out: list[dict] = []
        if kind == "card_testing":
            for i in range(self.rng.randint(6, 10)):
                t = ts + timedelta(seconds=i * self.rng.uniform(4, 12))
                out.append(self._txn(card, t, card.home, self.rng.uniform(0.5, 4.99), f"M{self.rng.randint(1000, 9999)}", "online_retail"))
        elif kind == "impossible_travel":
            away = self.rng.choice([c for c in CITIES if km(CITIES[c], CITIES[card.home]) > 1500])
            out.append(self._txn(card, ts, card.home, self.rng.uniform(20, 80), self.rng.choice(card.merchants), "restaurant"))
            out.append(self._txn(card, ts + timedelta(minutes=self.rng.uniform(5, 40)), away,
                                 self.rng.uniform(200, 1200), f"M{self.rng.randint(1000, 9999)}", "electronics"))
        elif kind == "benign_spree":  # a real cardholder at a mall: several new stores, moderate amounts
            for i in range(self.rng.randint(5, 8)):
                t = ts + timedelta(minutes=i * self.rng.uniform(0.6, 1.4))
                cat = self.rng.choice(["restaurant", "online_retail", "grocery"])
                out.append(self._txn(card, t, card.home, self.rng.uniform(15, 120), f"M{self.rng.randint(1000, 9999)}", cat))
        elif kind == "account_takeover":
            for i in range(self.rng.randint(7, 11)):
                t = ts + timedelta(seconds=i * self.rng.uniform(15, 30))
                cat = self.rng.choice(["electronics", "online_retail", "travel"])
                lo, hi = CATEGORIES[cat]
                out.append(self._txn(card, t, card.home, self.rng.uniform(lo, hi), f"M{self.rng.randint(1000, 9999)}", cat))
        self.truth.append({"attack_id": attack_id, "kind": kind, "card_id": card.card_id,
                           "start": out[0]["event_time"], "end": out[-1]["event_time"],
                           "txn_ids": [e["txn_id"] for e in out]})
        return out

    def run(self, minutes: int, tps: float, attacks_per_min: float) -> list[dict]:
        """Generate `minutes` of traffic, returned in *arrival* order (with lateness and duplicates)."""
        events: list[tuple[datetime, dict]] = []
        end = self.t + timedelta(minutes=minutes)
        t = self.t
        while t < end:
            t += timedelta(seconds=self.rng.expovariate(tps))
            events.append((t, self.normal(t)))
            if self.rng.random() < attacks_per_min / (60 * tps):
                kind = self.rng.choices(["card_testing", "impossible_travel", "account_takeover", "benign_spree"],
                                        [3, 3, 3, 2])[0]
                burst = [(datetime.fromisoformat(e["event_time"].replace("Z", "+00:00")), e) for e in self.attack(kind, t)]
                if burst[-1][0] < end:   # only keep attacks that finish inside the stream window
                    events += burst
                else:
                    self.truth.pop()

        arrivals = []
        for ts, e in events:
            delay = self.rng.expovariate(1 / 0.4)                      # normal network delay (s)
            if self.rng.random() < 0.02:
                delay += self.rng.uniform(5, 50)                     # late arrivals, within the watermark
            arrivals.append((ts + timedelta(seconds=delay), e))
            if self.rng.random() < 0.01:                              # duplicate delivery (retry)
                arrivals.append((ts + timedelta(seconds=delay + self.rng.uniform(0.1, 3)), e))
        arrivals.sort(key=lambda x: x[0])
        self.t = end
        return [e for _, e in arrivals]


# ---------------------------------------------------------------- sinks
def now_iso() -> str:
    """Wall-clock send time, used to measure end-to-end detection latency."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def write_files(events: list[dict], out_dir: str, batch_size: int, pause: float) -> None:
    os.makedirs(out_dir, exist_ok=True)
    for i in range(0, len(events), batch_size):
        tmp = os.path.join(out_dir, f".batch-{i // batch_size:06d}.tmp")
        sent_at = now_iso()
        with open(tmp, "w") as f:
            f.write("\n".join(json.dumps({**e, "sent_at": sent_at}) for e in events[i:i + batch_size]) + "\n")
        os.rename(tmp, os.path.join(out_dir, f"batch-{i // batch_size:06d}.json"))  # atomic for the file source
        if pause:
            time.sleep(pause)


def write_kafka(events: list[dict], bootstrap: str, topic: str, rate: float) -> None:
    from kafka import KafkaProducer

    p = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode(),
                      key_serializer=lambda k: k.encode(), linger_ms=20, acks="all")
    gap = 1 / rate if rate else 0
    for e in events:
        p.send(topic, key=e["card_id"], value={**e, "sent_at": now_iso()})   # keyed by card: one card's events stay in order per partition
        if gap:
            time.sleep(gap)
    p.flush()


def main() -> None:
    ap = argparse.ArgumentParser(description="Simulate card transactions with hidden fraud attacks")
    ap.add_argument("--minutes", type=int, default=30)
    ap.add_argument("--tps", type=float, default=40, help="normal transactions per second (event time)")
    ap.add_argument("--attacks-per-min", type=float, default=1.5)
    ap.add_argument("--cards", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--sink", choices=["files", "kafka"], default="files")
    ap.add_argument("--out", default="data/landing")
    ap.add_argument("--batch-size", type=int, default=2000)
    ap.add_argument("--pause", type=float, default=0.0, help="seconds between files (to watch it live)")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="card-transactions")
    ap.add_argument("--rate", type=float, default=0.0, help="kafka messages per second (0 = as fast as possible)")
    ap.add_argument("--truth", default="data/ground_truth/attacks.json")
    a = ap.parse_args()

    sim = Simulator(seed=a.seed, cards=a.cards)
    events = sim.run(a.minutes, a.tps, a.attacks_per_min)
    os.makedirs(os.path.dirname(a.truth), exist_ok=True)
    with open(a.truth, "w") as f:
        json.dump(sim.truth, f, indent=1)
    if a.sink == "files":
        write_files(events, a.out, a.batch_size, a.pause)
    else:
        write_kafka(events, a.bootstrap, a.topic, a.rate)
    kinds = {}
    for t in sim.truth:
        kinds[t["kind"]] = kinds.get(t["kind"], 0) + 1
    print(json.dumps({"events_sent": len(events), "attacks": kinds}, indent=2))


if __name__ == "__main__":
    main()
