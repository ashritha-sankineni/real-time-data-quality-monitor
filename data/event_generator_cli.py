"""
CLI Synthetic Event Generator (Local)
Writes newline-delimited JSON (ndjson) to a local path.

Example:
python data/event_generator_cli.py --out data/out/events.ndjson --events 200 --bad-rate 0.15 --sleep-ms 50
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any, List


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

STORE_IDS: List[int] = list(range(100, 121))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def base_event() -> Dict[str, Any]:
    sku = random.choice(SKUS)
    store_id = random.choice(STORE_IDS)
    quantity = random.randint(1, 6)

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
    if random.random() > bad_rate:
        return event

    issue = random.choice(
        ["NEGATIVE_PRICE", "MISSING_STORE_ID", "NULL_SKU", "QUANTITY_SPIKE", "LATE_EVENT_TIME"]
    )

    if issue == "NEGATIVE_PRICE":
        event["price"] = round(-abs(float(event["price"])), 2)
    elif issue == "MISSING_STORE_ID":
        event.pop("store_id", None)
    elif issue == "NULL_SKU":
        event["sku"] = None
    elif issue == "QUANTITY_SPIKE":
        event["quantity"] = random.randint(250, 1200)
    elif issue == "LATE_EVENT_TIME":
        days_ago = random.randint(2, 10)
        late_ts = datetime.now(timezone.utc).timestamp() - (days_ago * 86400)
        event["event_time"] = datetime.fromtimestamp(late_ts, tz=timezone.utc).isoformat(timespec="seconds")

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
    p.add_argument("--events", type=int, default=200, help="Total events")
    p.add_argument("--bad-rate", type=float, default=0.15, help="Bad event fraction (0-1)")
    p.add_argument("--sleep-ms", type=int, default=50, help="Sleep between events")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    ensure_outdir(args.out)
    if os.path.exists(args.out):
        os.remove(args.out)

    print(f"Writing events to: {args.out}")

    for i in range(1, args.events + 1):
        ev = inject_bad_data(base_event(), args.bad_rate)
        write_event(args.out, ev)
        if i % 50 == 0:
            print(f"Generated {i}/{args.events}")
        time.sleep(max(args.sleep_ms, 0) / 1000.0)

    print("Done.")


if __name__ == "__main__":
    main()
