-- 1) Overall daily DQ summary
SELECT *
FROM gold_dq_daily_summary
ORDER BY business_date DESC;

-- 2) Violations by rule (daily)
SELECT business_date, violation_reasons, violation_count
FROM gold_dq_daily_by_rule
ORDER BY business_date DESC, violation_count DESC;

-- 3) Top 20 stores causing invalid records
SELECT store_id, invalid_events
FROM gold_dq_worst_stores
ORDER BY invalid_events DESC
LIMIT 20;

-- 4) Top 20 SKUs causing invalid records
SELECT sku, invalid_events
FROM gold_dq_worst_skus
ORDER BY invalid_events DESC
LIMIT 20;

-- 5) Quick check: do injected issues align with violations?
SELECT injected_issue, violation_reasons, COUNT(*) AS cnt
FROM dq_violations
GROUP BY injected_issue, violation_reasons
ORDER BY cnt DESC;
