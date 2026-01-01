"""
Bronze ingestion (Batch micro-batches) for Databricks CE environments
where streaming + checkpointing + DBFS may be restricted.

Creates/overwrites a managed table: bronze_events
Appends additional batches to simulate streaming.
"""

from data.event_generator_spark import make_bronze_batch

def init_bronze_table(spark):
    spark.sql("DROP TABLE IF EXISTS bronze_events")
    batch = make_bronze_batch(spark, n_rows=500, bad_rate=0.15)
    batch.write.format("delta").mode("overwrite").saveAsTable("bronze_events")

def append_micro_batch(spark, n_rows=500, bad_rate=0.15):
    make_bronze_batch(spark, n_rows=n_rows, bad_rate=bad_rate) \
        .write.format("delta").mode("append").saveAsTable("bronze_events")
