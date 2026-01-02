# Streaming Module — Bronze Ingestion

This module is responsible for ingesting events into the **Bronze layer** of the data quality pipeline.

It supports **two ingestion approaches**:
- an **ideal, production-style streaming design** (conceptual)
- a **Databricks Community Edition–compatible batch fallback** (implemented)

Both approaches write data using the **same Bronze schema**, preserving downstream contracts.

---

## Responsibilities

The streaming module:
- Ingests raw events into the **Bronze layer**
- Preserves all fields (including bad data)
- Does **no validation or filtering**
- Acts as the single source for downstream DQ rules

---

## Ingestion Modes

### Mode A — Ideal / Production-Style Streaming (Conceptual)

**File**
bronze_ingest_streaming.py

**Use when**:
- File-based ingestion is supported
- Structured Streaming checkpoints are available
- DBFS or cloud storage is accessible

**Behavior**:
- Reads newline-delimited JSON (`ndjson`) events
- Uses an explicit schema
- Appends to Bronze Delta storage using streaming

This represents how the pipeline would run in a full-featured production environment.

> Note: This mode may not be runnable end-to-end in Databricks Community Edition due to platform constraints.

---

### Mode B — Databricks CE Compatible (Implemented & Tested)

**File**
bronze_ingest_batch.py

**Use when**:
- DBFS root is disabled
- Local filesystem access is restricted
- Streaming triggers or checkpoints are unavailable

**Approach**:
- Generate events directly inside Spark
- Append small batches to Bronze tables
- Repeat appends to simulate streaming behavior

**Key Functions**
```python
init_bronze_table(spark)
append_micro_batch(spark, n_rows=500, bad_rate=0.15)
```

This produces the managed table:bronze_events
## Bronze Data Characteristics
Contains all raw events.
Includes intentionally bad data.
Includes late-arriving events.
No records are dropped or corrected at this stage

Validation occurs downstream in dq_rules/.
