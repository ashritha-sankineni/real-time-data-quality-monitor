"""
Bronze ingestion (Ideal Streaming)
For environments that support file streaming + checkpoints.

Reads NDJSON from an input directory and writes Delta to a bronze path/table.

Note: Databricks CE restrictions may prevent running this; included for completeness.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

event_schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("sku", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("channel", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("txn_id", StringType(), True),
    StructField("injected_issue", StringType(), True),
])

def start_bronze_stream(spark, input_path: str, bronze_path: str, checkpoint_path: str):
    df = spark.readStream.schema(event_schema).json(input_path)

    return (
        df.writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", checkpoint_path)
          .start(bronze_path)
    )
