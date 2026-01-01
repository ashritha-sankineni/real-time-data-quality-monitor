# Real-Time Data Quality Monitor with LLM Explainability

An enterprise-style **data quality monitoring pipeline** built using **Spark + Delta Lake**, with clear separation of **Bronze / Silver / Gold layers** and optional **LLM-based explanations** for data issues.

This project is designed to be:
- **Production-inspired**
- **Runnable on Databricks Community Edition (with CE-compatible execution mode)**
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

### Conceptual Flow

```text
Event Generation
      ↓
Bronze Events (raw + issues)
      ↓
DQ Rules Engine
      ↓
Silver Events (valid data)     DQ Violations (invalid data)
      ↓
Gold Metrics (DQ observability)
