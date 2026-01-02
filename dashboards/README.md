# Dashboards & Gold Metrics

This module contains **Gold-layer metrics and queries** used to monitor **data quality health** across the pipeline.

These queries operate on the outputs of the DQ rules engine and are designed for:
- Observability
- Trend analysis
- Root-cause identification

---

## Purpose of the Gold Layer

The Gold layer answers the question:

> **“How healthy is our data today, and where are the problems?”**

It aggregates violations into **actionable metrics** rather than raw records.

---

##
dq_metrics.sql


They are intended to be run in Databricks SQL or a notebook.

---

## Key Metrics

### 1) Violations by Rule and Business Date

Aggregates invalid records by:
- `business_date` (derived from event timestamp)
- `violation_reasons`

This highlights:
- Which rules fail most often
- Whether issues are improving or worsening over time

Late-arriving events may appear under **earlier business dates**, which is expected behavior.

---

### 2) Daily Data Quality Summary

Compares:
- Total Bronze events
- Valid Silver events
- Invalid events

Produces:
- Daily invalid count
- Invalid rate (%)

This is the primary **data quality health KPI**.

---

### 3) Worst Offending Stores

Ranks stores by number of invalid records.

Useful for:
- Operational debugging
- Identifying upstream data producers causing issues

---

### 4) Worst Offending SKUs

Ranks SKUs by number of violations.

Helps detect:
- Product-level data issues
- Pricing or quantity anomalies

---

## Example Usage

In Databricks SQL or a notebook:

```sql
SELECT *
FROM gold_dq_daily_summary
ORDER BY business_date DESC;
SELECT violation_reasons, SUM(violation_count) AS total
FROM gold_dq_daily_by_rule
GROUP BY violation_reasons
ORDER BY total DESC;


## Design Notes

Metrics are derived from event time, not ingestion time
Late-arriving data is intentionally surfaced, not hidden
No alerts are triggered in this project (metrics only)

## Future Enhancements

Alerting (Slack / email)
Store-level anomaly thresholds
Time-windowed comparisons (WoW, MoM)
Visualization via Databricks SQL dashboards or BI tools
