 """
DQ Rules Engine: Bronze -> Silver + Violations
Creates managed tables:
- silver_events
- dq_violations
"""

from pyspark.sql import functions as F

def apply_dq_rules(spark):
    bronze = spark.table("bronze_events").withColumn("event_ts", F.to_timestamp("event_time"))

    dq = (
        bronze
        .withColumn("rule_missing_required",
                    F.col("store_id").isNull() | F.col("sku").isNull() | F.col("event_ts").isNull())
        .withColumn("rule_negative_price", F.col("price") < F.lit(0))
        .withColumn("rule_quantity_spike", F.col("quantity") >= F.lit(250))
        .withColumn("rule_late_event",
                    F.col("event_ts") < (F.current_timestamp() - F.expr("INTERVAL 2 DAYS")))
        .withColumn("is_valid",
                    ~(F.col("rule_missing_required") | F.col("rule_negative_price") |
                      F.col("rule_quantity_spike") | F.col("rule_late_event")))
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
    )

    silver = dq.filter(F.col("is_valid") == True) \
               .drop("rule_missing_required","rule_negative_price","rule_quantity_spike","rule_late_event","is_valid","violation_reasons")

    violations = dq.filter(F.col("is_valid") == False)

    spark.sql("DROP TABLE IF EXISTS silver_events")
    spark.sql("DROP TABLE IF EXISTS dq_violations")

    silver.write.format("delta").mode("overwrite").saveAsTable("silver_events")
    violations.write.format("delta").mode("overwrite").saveAsTable("dq_violations")

    return {"silver_count": silver.count(), "violation_count": violations.count()}
