# Real-Time Data Quality Monitor

A **production-inspired data quality monitoring pipeline** built with **Spark and Delta Lake**, demonstrating how to detect, isolate, and measure data quality issues across **Bronze, Silver, and Gold** layers.

The project also includes an **optional LLM-based explainability layer**, implemented as a **decoupled external service** to accommodate platform constraints.

This project is designed to be:
- Runnable on **Databricks Community Edition**
- Adaptable to **real-world platform constraints**
- Modular, readable, and **portfolio-ready**

---

## Problem Statement

Data pipelines rarely fail loudly.  
They fail silently — by allowing **bad data** to flow downstream into analytics, dashboards, and ML models.

This project demonstrates how to:
- Detect bad data early
- Enforce rule-based validation
- Separate valid data from violations
- Track data quality health over time
- Communicate issues clearly to both technical and non-technical stakeholders

---

## High-Level Architecture

```text
Event Generation
      ↓
Bronze Events
      ↓
DQ Rules Engine
      ↓
Silver Events        DQ Violations
      ↓
Gold DQ Metrics
```

The architecture remains consistent, even when execution details change due to platform limitations.

## Execution Overview

This repository intentionally supports two execution modes.

#### Mode A — Ideal / Production-Style (Conceptual)

Structured Streaming ingestion

File-based sources

Continuous processing with checkpoints

Included to demonstrate how this pipeline would run in a fully featured production environment.

#### Mode B — Databricks Community Edition Compatible (Implemented)

Batch micro-ingestion

Spark-native event generation

No reliance on DBFS root or streaming triggers

This mode is fully runnable in Databricks CE and preserves the same data contracts and layer boundaries.

Implementation details are documented in the module-level READMEs.

## Repository Guide
| Folder                                      | Purpose                                                           |
| ------------------------------------------- | ----------------------------------------------------------------- |
| [`data/`](data/README.md)                   | Synthetic retail event generation (local + Spark-native)          |
| [`streaming/`](streaming/README.md)         | Bronze ingestion logic (conceptual streaming + CE batch fallback) |
| [`dq_rules/`](dq_rules/README.md)           | Rule-based data quality validation (Bronze → Silver + Violations) |
| [`dashboards/`](dashboards/README.md)       | Gold metrics and observability SQL                                |
| [`llm_explainer/`](llm_explainer/README.md) | External LLM-based explainability (Groq)                          |

## How to Get Started (Databricks Community Edition)

Generate Bronze data
Run the batch micro-ingestion workflow
→ See: streaming/README.md

Apply Data Quality rules
Produce Silver data and DQ violations
→ See: dq_rules/README.md

Analyze Gold metrics
Query daily data quality health and trends
→ See: dashboards/README.md

(Optional) Generate LLM-based summaries
Run the external Groq explainer locally
→ See: llm_explainer/README.md

## LLM Explainability (Optional, Decoupled)

Due to outbound network restrictions in Databricks Community Edition, LLM integration is implemented as a decoupled external component.

Spark handles deterministic detection and enforcement

An external script uses an LLM (Groq) to generate:

Plain-English incident summaries

Technical triage checklists

Recommended actions

This mirrors real-world enterprise architecture, where LLMs are used for explainability and communication, not enforcement.

### Why This Matters

This project reflects how real data platforms are built and operated:

Architecture is stable

Execution adapts to constraints

Data contracts are preserved

LLMs are used responsibly and cost-effectively

The result is a realistic, end-to-end example of modern data quality engineering.

### Author

####Ashritha Sankineni
  Senior Data Engineer
  Spark • Delta Lake • Data Quality • Observability • LLMs
