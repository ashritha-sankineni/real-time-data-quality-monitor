# Real-Time Data Quality Monitor with LLM Explainability

Enterprise-style real-time data quality monitoring built using Spark Structured Streaming and Delta Lake, with optional LLM-based explanations for detected data anomalies. Designed to run on Databricks Community Edition (free).

---

## Why this project?

Most data pipelines don’t fail loudly.  
They fail quietly by allowing bad data to flow into dashboards, ML models, and business decisions.

This project demonstrates how to:
- Detect data quality issues in real time
- Persist quality metrics for observability
- Explain issues in plain English using an LLM

---

## Features

- Real-time ingestion using Spark Structured Streaming
- Rule-based data quality checks (nulls, ranges, spikes)
- Medallion architecture (Bronze → Silver → Gold)
- Delta Lake storage
- Data quality metrics and violations table
- LLM-based explanations (optional)

---

## Architecture

![Architecture](assets/architecture.png)

Flow:
- Bronze: Raw streaming ingestion
- Silver: Valid records and quarantined invalid records
- Gold: Aggregated data quality metrics
- LLM Explainer: Human-readable explanations

---

## Tech Stack

- Databricks Community Edition
- Spark Structured Streaming
- Delta Lake
- Python
- SQL
- Optional LLM: OpenAI, Groq, or HuggingFace

---

## Repository Structure

```text
data/          - synthetic event generator
streaming/     - ingestion and transformations
dq_rules/      - validation and anomaly rules
llm_explainer/ - LLM prompts and explanations
dashboards/    - SQL queries for metrics
assets/        - diagrams and screenshots

---


Planned Data Quality Rules

Negative price detection

Quantity spike anomalies

Missing required fields

Late-arriving data (freshness)

How to Run

Instructions will be added once the notebooks are implemented.

Future Enhancements

Alerting via Slack or email

Schema drift detection

Great Expectations integration

Unity Catalog compatibility

