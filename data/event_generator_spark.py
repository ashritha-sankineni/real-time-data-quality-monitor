"""
Spark-native Event Generator (Databricks-safe)
Creates a DataFrame of synthetic events (no local files, no DBFS dependency).

Use this for environments where:
- DBFS root is disabled
- local FS access is forbidden
- streaming checkpoints are restricted

This returns a DataFrame you can append into a Bronze Delta table.
"""

from pyspark.sql import functions as F

SKUS = ["BREAD_WHITE","MILK_1L","EGGS_DOZEN","RICE_5LB","CHIPS_CLASSIC","COFFEE_GROUND","YOGURT_GREEK","BANANA_BUNCH"]
CHANNELS = ["in_store", "online"]
PAYMENTS = ["card", "cash", "mobile_wallet"]

def _pick(arr):
    return F.element_at(F.array(*[F.lit(x) for x in arr]), (F.floor(F.rand()*len(arr)) + 1).cast("int"))

def make_bronze_batch(spark, n_rows: int = 500, bad_rate: float = 0.15):
    base = (
        spark.range(n_rows)
        .withColumn("event_ts", F.current_timestamp())
        .withColumn("event_time", F.col("event_ts").cast("string"))
        .withColumn("store_id", (F.floor(F.rand()*21) + 100).cast("int"))
        .withColumn("sku", _pick(SKUS))
        .withColumn("channel", _pick(CHANNELS))
        .withColumn("payment_type", _pick(PAYMENTS))
        .withColumn("txn_id", F.concat(F.lit("TXN-"), (F.floor(F.rand()*9000000) + 1000000).cast("int").cast("string")))
        .withColumn("quantity", (F.floor(F.rand()*6) + 1).cast("int"))
        .withColumn(
            "price_base",
            F.when(F.col("sku")=="BREAD_WHITE", F.lit(3.49))
             .when(F.col("sku")=="MILK_1L", F.lit(4.29))
             .when(F.col("sku")=="EGGS_DOZEN", F.lit(5.99))
             .when(F.col("sku")=="RICE_5LB", F.lit(9.49))
             .when(F.col("sku")=="CHIPS_CLASSIC", F.lit(4.99))
             .when(F.col("sku")=="COFFEE_GROUND", F.lit(11.99))
             .when(F.col("sku")=="YOGURT_GREEK", F.lit(1.79))
             .otherwise(F.lit(2.49))
        )
        .withColumn("price", F.round(F.col("price_base") * (F.rand()*0.3 + 0.85), 2))
        .drop("price_base")
    )

    with_issues = (
        base
        .withColumn("issue_roll", F.rand())
        .withColumn(
            "injected_issue",
            F.when(F.col("issue_roll") > bad_rate, F.lit(None))
             .otherwise(
                F.element_at(
                    F.array(
                        F.lit("NEGATIVE_PRICE"),
                        F.lit("MISSING_STORE_ID"),
                        F.lit("NULL_SKU"),
                        F.lit("QUANTITY_SPIKE"),
                        F.lit("LATE_EVENT_TIME"),
                    ),
                    (F.floor(F.rand()*5) + 1).cast("int")
                )
             )
        )
        .withColumn("price",
            F.when(F.col("injected_issue")=="NEGATIVE_PRICE", -F.abs(F.col("price")))
             .otherwise(F.col("price"))
        )
        .withColumn("store_id",
            F.when(F.col("injected_issue")=="MISSING_STORE_ID", F.lit(None).cast("int"))
             .otherwise(F.col("store_id"))
        )
        .withColumn("sku",
            F.when(F.col("injected_issue")=="NULL_SKU", F.lit(None).cast("string"))
             .otherwise(F.col("sku"))
        )
        .withColumn("quantity",
            F.when(F.col("injected_issue")=="QUANTITY_SPIKE", (F.floor(F.rand()*951) + 250).cast("int"))
             .otherwise(F.col("quantity"))
        )
        .withColumn("event_time",
            F.when(F.col("injected_issue")=="LATE_EVENT_TIME",
                   F.date_format(F.expr("event_ts - INTERVAL 5 DAYS"), "yyyy-MM-dd HH:mm:ss"))
             .otherwise(F.col("event_time"))
        )
        .drop("issue_roll")
        .drop("event_ts")
    )

    return with_issues
