"""
External Groq Explainer (Runs outside Databricks)

Purpose:
- Pull a small aggregated DQ summary (from CSV export or manual input)
- Ask Groq to produce a human-readable narrative:
  - What happened
  - What changed
  - What to check
  - Suggested actions

Why external?
- Databricks Community Edition often blocks outbound internet.
- In production, this runs as a small service (Lambda/Function/Job) outside Spark.

Usage:
  export GROQ_API_KEY="..."
  python llm_explainer/groq_explain_summary.py --input dq_summary.csv
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd

try:
    from groq import Groq
except ImportError:
    print("Missing dependency: groq. Install with: pip install groq pandas", file=sys.stderr)
    raise


DEFAULT_MODEL = "llama-3.1-8b-instant"


def build_prompt(business_date: str, summary_rows: pd.DataFrame, top_n: int = 10) -> str:
    """
    summary_rows expected columns (flexible):
      - violation_reasons (or rule)
      - violation_count (or count)
      - invalid_rate (optional)
      - total_events (optional)
      - invalid_events (optional)
    """
    # Normalize column names if needed
    cols = {c.lower(): c for c in summary_rows.columns}

    rule_col = cols.get("violation_reasons") or cols.get("rule") or cols.get("reason") or None
    count_col = cols.get("violation_count") or cols.get("count") or cols.get("cnt") or None

    if not rule_col or not count_col:
        raise ValueError(
            f"Input must include rule + count columns. Found: {list(summary_rows.columns)}. "
            "Try headers like: violation_reasons, violation_count"
        )

    # Top N rules
    top = summary_rows.sort_values(count_col, ascending=False).head(top_n)
    top_lines = "\n".join([f"- {r[rule_col]}: {int(r[count_col])}" for _, r in top.iterrows()])

    # Optional global KPIs if present
    total_events = summary_rows[cols["total_events"]].iloc[0] if "total_events" in cols else None
    invalid_events = summary_rows[cols["invalid_events"]].iloc[0] if "invalid_events" in cols else None
    invalid_rate = summary_rows[cols["invalid_rate"]].iloc[0] if "invalid_rate" in cols else None

    kpi_block = ""
    if total_events is not None or invalid_events is not None or invalid_rate is not None:
        kpi_block = "\nKPI snapshot:\n"
        if total_events is not None:
            kpi_block += f"- total_events: {total_events}\n"
        if invalid_events is not None:
            kpi_block += f"- invalid_events: {invalid_events}\n"
        if invalid_rate is not None:
            kpi_block += f"- invalid_rate: {invalid_rate}\n"

    prompt = f"""
You are a senior data engineer writing a short Data Quality incident summary for stakeholders.

Context:
- We run a Spark + Delta Lake pipeline with Bronze/Silver/Gold.
- `dq_violations` captures invalid records with rule reasons.
- The goal is to produce a clear narrative: what happened, impact, likely causes, and next actions.

Business date: {business_date}

Top violation reasons and counts:
{top_lines}
{kpi_block}

Write:
1) A 4–6 sentence plain-English summary (non-technical audience friendly).
2) A concise technical triage checklist (3–6 bullets).
3) A recommended next action owner (e.g., "Upstream POS team", "Ingestion job owner", "Data platform team") based on the likely cause.

Constraints:
- Be specific but do not invent systems or exact upstream names.
- If "LATE_EVENT_TIME" appears, explain event-time vs ingestion-time briefly.
- If "MISSING_REQUIRED_FIELDS" dominates, suggest schema/mapping validation.
""".strip()

    return prompt


def call_groq(prompt: str, model: str = DEFAULT_MODEL, temperature: float = 0.2) -> str:
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError("GROQ_API_KEY is not set. Export it first: export GROQ_API_KEY='...'")

    client = Groq(api_key=api_key)

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a helpful assistant specialized in data engineering incident communication."},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
    )

    return resp.choices[0].message.content


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to CSV file with aggregated DQ metrics (by rule).")
    parser.add_argument("--business-date", default=None, help="Business date for the summary (YYYY-MM-DD).")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Groq model (default: {DEFAULT_MODEL})")
    parser.add_argument("--top-n", type=int, default=10, help="Top N rules to include in the prompt.")
    args = parser.parse_args()

    df = pd.read_csv(args.input)

    business_date = args.business_date
    if not business_date:
        # Best-effort: find a business_date column, else use today
        lower_cols = [c.lower() for c in df.columns]
        if "business_date" in lower_cols:
            business_date = df[df.columns[lower_cols.index("business_date")]].iloc[0]
        else:
            business_date = datetime.utcnow().strftime("%Y-%m-%d")

    prompt = build_prompt(str(business_date), df, top_n=args.top_n)
    output = call_groq(prompt, model=args.model)

    print("\n=== Groq DQ Summary ===\n")
    print(output)
    print("\n======================\n")


if __name__ == "__main__":
    main()
