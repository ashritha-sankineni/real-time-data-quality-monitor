# Data Quality Rules Module

This module applies **rule-based data quality validation** to events in the **Bronze layer**, producing:

- **Silver events** — records that pass all quality checks
- **DQ violations** — records that fail one or more rules, with detailed reasons

Invalid data is **isolated, not corrected**.

---

## Responsibilities

The DQ rules module:
- Reads raw events from the Bronze table
- Evaluates data quality rules independently
- Separates valid and invalid records
- Captures rule-level metadata for observability
- Writes clean Silver output and a violations table

---

## Input and Output Tables

| Layer | Table | Description |
|------|------|-------------|
| Input | `bronze_events` | Raw events (may contain bad data) |
| Output | `silver_events` | Records that passed all DQ rules |
| Output | `dq_violations` | Records that failed one or more rules |

---

## Implemented Data Quality Rules

| Rule | Condition |
|------|----------|
| Missing required fields | `store_id`, `sku`, or `event_time` is null |
| Negative price | `price < 0` |
| Quantity spike | `quantity ≥ 250` |
| Late-arriving event | Event timestamp older than 2 days |

Each rule is evaluated independently.  
A single record may violate **multiple rules**.

---

## Violation Metadata

For every invalid record, the following metadata is captured:

- Boolean flags per rule  
- Consolidated `violation_reasons` string  
- Run-level metadata:
  - `dq_run_id`
  - `dq_created_at`

This supports:
- Auditing and debugging
- Historical comparisons
- Gold-layer metric aggregation

---

## Core Entry Point

**File**
apply_dq_rules.py

## Behavior
Reads from bronze_events
Applies all configured DQ rules
## Overwrites:
silver_events
dq_violations

Returns record counts and run identifier

## Design Principles

Fail fast — detect issues early in the pipeline
Isolate bad data — do not silently drop or auto-correct
Preserve context — retain original values and failure reasons
Measure quality — violations feed Gold-layer observability
