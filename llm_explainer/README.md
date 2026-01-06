# LLM Explainer (Groq) — External Data Quality Summary

This module generates a **human-readable incident summary** for data quality issues using an LLM (Groq).

## Why is this external?

Databricks Community Edition (CE) often restricts outbound network access (DNS/egress).  
To keep the project **fully runnable in CE**, the Spark pipeline produces deterministic data quality outputs (tables/metrics), while the LLM step runs **outside Databricks** as a small external script.

This mirrors real-world architecture:
- **Spark/Databricks** = detection + enforcement
- **External service** = summarization + stakeholder communication

---

## What it produces

Given aggregated violations (by rule), the script generates:

1) **Plain-English Summary** (4–6 sentences)  
2) **Technical Triage Checklist** (3–6 bullets)  
3) **Recommended Next Action Owner** (who should investigate)

---

## Files

- `groq_explain_summary.py` — external script that calls Groq and prints the narrative
- `sample_dq_summary.csv` — example aggregated input to test locally

---

## Input format (CSV)

Minimum required columns:

| Column | Description |
|---|---|
| `business_date` | Date of the DQ run (YYYY-MM-DD) |
| `violation_reasons` | Rule name (e.g., `MISSING_REQUIRED_FIELDS`) |
| `violation_count` | Count of violations for that rule |

Example:
```csv
business_date,violation_reasons,violation_count
2026-01-01,MISSING_REQUIRED_FIELDS,61
2026-01-01,NEGATIVE_PRICE,31
2026-01-01,QUANTITY_SPIKE,24
2025-12-27,LATE_EVENT_TIME,29

```

Export the result to CSV (copy/paste download) and run the script locally.

Best practice: send aggregated metrics to the LLM (cheap + low risk) rather than sending raw row-level data.

2) Set your Groq API key
```
zsh/bash
export GROQ_API_KEY="YOUR_KEY"
```
