# Data Module — Synthetic Event Generation

This folder generates **synthetic retail transaction events** used to validate the downstream **data quality pipeline** (Bronze → Silver → Gold).

Events intentionally include **data quality issues** (e.g., missing fields, negative values) so the rules engine can detect and isolate bad records.

---

## Event Schema (fields)

Each event represents a retail transaction and typically includes:

- `event_time` (string timestamp)
- `store_id` (int)
- `sku` (string)
- `price` (double)
- `quantity` (int)
- `channel` (string: `in_store` / `online`)
- `payment_type` (string)
- `txn_id` (string)
- `injected_issue` (optional string)

---

## Injected Data Quality Issues

A configurable percentage of events include injected issues:

| Issue | Description |
|------|-------------|
| `NEGATIVE_PRICE` | Price is negative |
| `MISSING_STORE_ID` | `store_id` is removed / null |
| `NULL_SKU` | `sku` is null |
| `QUANTITY_SPIKE` | Abnormally large quantity (≥ 250) |
| `LATE_EVENT_TIME` | Event timestamp older than expected |

These issues are later detected by rule-based validation in `dq_rules/`.

---

## Two Generator Implementations

This repo includes two generators to support different environments.

---

### 1) `event_generator_cli.py` — Local / File-Based Generator

**Use when**: you can write local files and want to simulate file-based ingestion.

**Output**: newline-delimited JSON file (`.ndjson`), e.g.:
- `data/out/events.ndjson`

**Run**
```bash
python data/event_generator_cli.py \
  --out data/out/events.ndjson \
  --events 200 \
  --bad-rate 0.15 \
  --sleep-ms 50
```

## event_generator_spark.py — Spark-Native Generator (Databricks CE Compatible)

Use when: DBFS root or local filesystem access is restricted (common in Databricks CE setups).

Behavior:

Generates events directly inside Spark

Returns a DataFrame

Supports repeated small-batch appends to simulate streaming



## Why Two Generators?

Different environments impose different constraints.

This design keeps:

the same event contract

the same Bronze/Silver/Gold architecture

flexible execution paths (file-based or Spark-native)
