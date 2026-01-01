"""
Synthetic Event Generator (Retail)
- Generates realistic sales events for streaming demos
- Intentionally injects "bad data" to test DQ rules

Output:
- Writes newline-delimited JSON (ndjson) to: data/out/events.ndjson
  Each line is one JSON event, easy for Spark to read as a stream.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any, List


# -----------------------------
# Config: adjust as you like
# -----------------------------
SKUS: List[str] = [
    "BREAD_WHITE",
    "MILK_1L",
    "EGGS_DOZEN",
    "RICE_5LB",
    "CHIPS_CLASSIC",
    "COFFEE_GROUND",
    "YOGURT_GREEK",
    "BANANA_BUNCH",
]

STORE_IDS: List[int] = list(range(100, 121))  # 21 stores


def utc_now_iso() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def base_event() -> Dict[str, Any]:
    """Create a clean (valid) event."""
    sku = random.choice(SKUS)
    store_id = random.choice(STORE_IDS)
    quantity = random.randint(1, 6)

    # Simple SKU-based pricing baseline
    base_price = {
        "BREAD_WHITE": 3.49,
        "MILK_1L": 4.29,
        "EGGS_DOZEN": 5.99,
        "RICE_5LB": 9.49,
        "CHIPS_CLASSIC": 4.99,
        "COFFEE_GROUND": 11.99,
        "YOGURT_GREEK": 1.79,
        "BANANA_BUNCH": 2.49,
    }[sku]

    # Add small random variation (+/- up to 15%)
    price = round(base_price * random.uniform(0.85, 1.15), 2)

    return {
        "event_time": utc_now_iso(),
        "store_id": store_id,
        "sku": sku,
        "price": price,
        "quantity": quantity,
        "channel": random.choice(["in_store", "online"]),
        "payment_type": random.choice(["card", "cash", "mobile_wallet"]),
        "txn_id": f"TXN-{random.randint(1000000, 9999999)}",
    }


def inject_bad_data(event: Dict[str, Any], bad_rate: float) -> Dict[str, Any]:
    """
    With probability bad_rate, mutate event to include a data quality issue.
    Returns (possibly) mutated event.
    """
    if random.random() > bad_rate:
        return event

    issue = random.choice(
        [
            "NEGATIVE_PRICE",
            "MISSING_STORE_ID",
            "NULL_SKU",
            "QUANTITY_SPIKE",
            "LATE_EVENT_TIME",
        ]
    )

    if issue == "NEGATIVE_PRICE":
        event["price"] = round(-abs(float(event["price"])), 2)

    elif issue == "MISSING_STORE_ID":
        event.pop("store_id", None)

    elif issue == "NULL_SKU":
        event["sku"] = None

    elif issue == "QUANTITY_SPIKE":
        # abnormal spike
        event["quantity"] = random.randint(250, 1200)

    elif issue == "LATE_EVENT_TIME":
        # simulate late-arriving event: 2–10 days old
        days_ago = random.randint(2, 10)
        late_ts = datetime.now(timezone.utc).timestamp() - (days_ago * 86400)
        event["event_time"] = datetime.fromtimestamp(late_ts, tz=timezone.utc).isoformat(timespec="seconds")

    # Tag the injected issue so it's easy to validate your pipeline later
    event["injected_issue"] = issue
    return event


def ensure_outdir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def write_event(path: str, event: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic retail events (ndjson).")
    p.add_argument("--out", default="data/out/events.ndjson", help="Output ndjson file path")
    p.add_argument("--events", type=int, default=200, help="Total number of events to generate")
    p.add_argument("--bad-rate", type=float, default=0.15, help="Fraction of events with injected bad data (0-1)")
    p.add_argument("--sleep-ms", type=int, default=50, help="Sleep between events to mimic streaming")
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    ensure_outdir(args.out)

    # Start fresh each run (makes demos easier)
    if os.path.exists(args.out):
        os.remove(args.out)

    print(f"Writing events to: {args.out}")
    print(f"Total events: {args.events} | bad_rate: {args.bad_rate} | sleep_ms: {args.sleep_ms}")

    for i in range(1, args.events + 1):
        ev = base_event()
        ev = inject_bad_data(ev, args.bad_rate)
        write_event(args.out, ev)

        if i % 50 == 0:
            print(f"Generated {i}/{args.events} events...")

        time.sleep(max(args.sleep_ms, 0) / 1000.0)

    print("Done.")


if __name__ == "__main__":
    main()
