# Real-Time Data Quality Monitor with LLM Explainability

An enterprise-style **data quality monitoring pipeline** built using **Spark + Delta Lake**, with clear separation of **Bronze / Silver / Gold layers** and optional **LLM-based explanations** for data issues.

This project is designed to be:
- **Production-inspired**
- **Runnable on Databricks Community Edition**
- **Adaptable to constrained environments**
- **Portfolio-ready for Senior Data Engineer roles**

---

## Why this project?

Most data pipelines don’t fail loudly —  
they fail quietly by letting **bad data** flow into analytics, dashboards, and ML models.

This project demonstrates how to:
- Detect data quality issues early
- Enforce rule-based validation
- Separate valid data from violations
- Track quality metrics over time
- Adapt architecture based on platform constraints

---

## Key Features

- Synthetic retail event generation with injected data issues
- Bronze / Silver / Gold data modeling using Delta Lake
- Rule-based data quality checks:
  - Missing required fields
  - Negative values
  - Quantity spikes
  - Late-arriving data
- Dedicated **violations table** with readable failure reasons
- Daily data quality metrics for observability
- Two execution modes:
  - Ideal streaming-style design
  - Databricks CE–compatible batch fallback

---

## Architecture Overview

**Conceptual Flow**

Event Generation
↓
Bronze Events (raw + issues)
↓
DQ Rules Engine
↓
Silver Events (valid data) Violations (invalid data)
↓
Gold Metrics (DQ observability)



The **architecture stays the same**, even when execution mode changes.

---

## Execution Modes (Two-track Design)

This repo intentionally supports **two execution paths**.

### Mode A — Ideal / Production-Style (Conceptual)
Use when your environment supports:
- File-based ingestion
- Structured Streaming
- Checkpointing

**Components**
- `data/event_generator_cli.py`
- `streaming/bronze_ingest_streaming.py`

This mode demonstrates how the pipeline would run in a full-featured production environment.

> Note: Databricks Community Edition restrictions may prevent this mode from running end-to-end.

---

### Mode B — Databricks CE Compatible (Implemented & Tested)

Used when:
- DBFS root is disabled
- Local filesystem access is restricted
- Streaming triggers/checkpoints are unavailable

**Approach**
- Generate events directly inside Spark
- Append small batches to simulate streaming
- Preserve Bronze / Silver / Gold contracts

**Components**
- `data/event_generator_spark.py`
- `streaming/bronze_ingest_batch.py`
- `dq_rules/apply_dq_rules.py`

This mirrors real-time behavior while remaining fully executable in constrained environments.

---

## Repository Structure

data/
event_generator_cli.py # Local NDJSON generator
event_generator_spark.py # Spark-native generator (CE-safe)
README.md

streaming/
bronze_ingest_streaming.py # Ideal streaming ingestion (conceptual)
bronze_ingest_batch.py # CE-compatible batch ingestion
init.py

dq_rules/
apply_dq_rules.py # Bronze → Silver + Violations
init.py

dashboards/
dq_metrics.sql # Gold metrics & analysis queries

assets/
architecture diagrams
screenshots


---

## Data Quality Rules Implemented

| Rule | Description |
|----|------------|
| Missing required fields | `store_id`, `sku`, or `event_time` is null |
| Negative price | Price < 0 |
| Quantity spike | Quantity ≥ 250 |
| Late-arriving event | Event older than 2 days |

Violations are captured with:
- Rule flags
- Human-readable `violation_reasons`
- Run-level metadata (`dq_run_id`, timestamps)

---

## Gold Metrics (Examples)

- Daily total events vs invalid events
- Invalid rate (%) by day
- Violations by rule
- Worst offending stores
- Worst offending SKUs

Queries are available in:
dashboards/dq_metrics.sql


---

## Design Decisions & Tradeoffs

- Streaming constraints in Databricks CE required a batch-based fallback
- Architecture and data contracts were preserved despite execution changes
- Code is modularized to support both constrained and ideal environments

This reflects real-world data engineering work: **adapting without breaking design intent**.

---

## Future Enhancements

- LLM-based explanations for violations
- Alerting (Slack / email)
- Schema drift detection
- Integration with Great Expectations
- Unity Catalog compatibility

---

## Author

**Ashritha Sankineni**  
Senior Data Engineer  
Spark • Delta Lake • Databricks • Data Quality • Observability

