"""
Data Quality Rules Engine

Applies rule-based validation to Bronze events and produces:
- silver_events (valid records)
- dq_violations (invalid records with reasons)

Designed for Databricks / Spark environments using Delta tables.
"""

from pyspark.sql import functions as F
import uuid


def apply_dq_rules(spark,
                   bronze_table: str = "bronze_events",
                   silver_table: str = "silver_events",
                   violations_table: str = "dq_violations",
                   late_days: int = 2,
                   quantity_spike_threshold: int = 250):
    """
    Reads Bronze events, applies DQ rules, and writes Silver + Violations tables.

    Args:
        spark: SparkSession
        bronze_table: source table name
        silver_table: output valid table name
        violations_table: output violations table name
        late_days: threshold for late-arriving data (days)
        quantity_spike_threshold: threshold for quantity spike

    Returns:
        dict with counts and dq_run_id
    """
    run_id = str(uuid.uuid4())

    bronze = (
        spark.table(bronze_table)
             .withColumn("event_ts", F.to_timestamp("event_time"))
    )

    dq = (
        bronze
        # Rule flags
        .withColumn(
            "rule_missing_required",
            F.col("store_id").isNull() | F.col("sku").isNull() | F.col("event_ts").isNull()
        )
        .withColumn("rule_negative_price", F.col("price") < F.lit(0))
        .withColumn("rule_quantity_spike", F.col("quantity") >= F.lit(quantity_spike_threshold))
        .withColumn(
            "rule_late_event",
            F.col("event_ts") < (F.current_timestamp() - F.expr(f"INTERVAL {late_days} DAYS"))
        )
        # Validity
        .withColumn(
            "is_valid",
            ~(
                F.col("rule_missing_required")
                | F.col("rule_negative_price")
                | F.col("rule_quantity_spike")
                | F.col("rule_late_event")
            )
        )
        # Consolidated reason string (supports multiple failures)
        .withColumn(
            "violation_reasons",
            F.concat_ws(
                ", ",
                F.when(F.col("rule_missing_required"), F.lit("MISSING_REQUIRED_FIELDS")),
                F.when(F.col("rule_negative_price"), F.lit("NEGATIVE_PRICE")),
                F.when(F.col("rule_quantity_spike"), F.lit("QUANTITY_SPIKE")),
                F.when(F.col("rule_late_event"), F.lit("LATE_EVENT_TIME")),
            )
        )
        # Run metadata
        .withColumn("dq_run_id", F.lit(run_id))
        .withColumn("dq_created_at", F.current_timestamp())
    )

    silver = (
        dq.filter(F.col("is_valid") == True)
          .drop(
              "rule_missing_required",
              "rule_negative_price",
              "rule_quantity_spike",
              "rule_late_event",
              "is_valid",
              "violation_reasons",
          )
    )

    violations = dq.filter(F.col("is_valid") == False)

    # Overwrite outputs for reproducibility (portfolio-friendly)
    spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
    spark.sql(f"DROP TABLE IF EXISTS {violations_table}")

    silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)
    violations.write.format("delta").mode("overwrite").saveAsTable(violations_table)

    return {
        "dq_run_id": run_id,
        "silver_count": silver.count(),
        "violation_count": violations.count(),
    }
