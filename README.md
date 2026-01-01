# Real-Time Data Quality Monitor

A **production-inspired data quality monitoring pipeline** built with **Spark and Delta Lake**, demonstrating how to detect, isolate, and measure data quality issues across Bronze, Silver, and Gold layers.

This project is designed to be:
- Runnable on **Databricks Community Edition**
- Adaptable to **platform constraints**
- Clear, modular, and **portfolio-ready**

---

## Problem Statement

Data pipelines rarely fail loudly.  
They fail silently — by allowing bad data to flow downstream.

This project demonstrates how to:
- Detect bad data early
- Enforce rule-based validation
- Separate valid data from violations
- Track data quality health over time

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



## Execution Overview

This repo supports two execution modes:

Ideal / Production-style (conceptual streaming design)

Databricks CE–compatible (batch micro-ingestion fallback)

Implementation details are documented within each module.

## Repository Guide


| Folder        | Purpose                         |
| ------------- | ------------------------------- |
| `data/`       | Synthetic event generation      |
| `streaming/`  | Bronze ingestion logic          |
| `dq_rules/`   | Data quality rules & validation |
| `dashboards/` | Gold metrics & SQL queries      |
| `docs/`       | Platform constraints & notes    |

## How to Get Started (Databricks CE)

Generate Bronze data (batch micro-ingestion)

Apply DQ rules to produce Silver + Violations

Query Gold metrics for observability

➡️ See module-level READMEs for step-by-step instructions.

## Why This Matters

This project reflects real-world data engineering:

Design stays stable

Execution adapts to constraints

Data contracts are preserved
