# pylint: disable=E1101
"""
Centralized DQ logging script for Fabric pipeline.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


def log(stage: str, dataset: str, count: int):
    """
    Logs rowcounts before and after cleaning into dbo.dq_rowcount_logs.
    """
    spark = SparkSession.builder.getOrCreate()

    # 1. Ensure table exists in dbo schema
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dbo.dq_rowcount_logs (
            dataset STRING,
            stage STRING,
            row_count BIGINT,
            log_timestamp TIMESTAMP
        )
        """
    )

    # 2. Append using DataFrame API
    data = [(dataset, stage, int(count))]
    df = spark.createDataFrame(data, ["dataset", "stage", "row_count"])

    df.withColumn("log_timestamp", current_timestamp()) \
      .write.format("delta") \
      .mode("append") \
      .saveAsTable("dbo.dq_rowcount_logs")

    print(f"[ROWCOUNT] {dataset} | {stage} | {count}")


def log_invalid(dataset: str, temp_view_name: str = "all_invalid"):
    """
    Logs aggregated invalid row counts into dbo.dq_invalid_summary_logs.
    """
    spark = SparkSession.builder.getOrCreate()

    if not spark.catalog.tableExists(temp_view_name):
        print(
            f"[WARNING] Temp view '{temp_view_name}' not found. Logging 0 invalid rows.")
        count = 0
    else:
        count = spark.table(temp_view_name).count()

    # 1. Ensure summary table exists in dbo schema
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dbo.dq_invalid_summary_logs (
            dataset STRING,
            invalid_rows BIGINT,
            log_timestamp TIMESTAMP
        )
        """
    )

    # 2. Append using DataFrame API
    data = [(dataset, int(count))]
    df = spark.createDataFrame(data, ["dataset", "invalid_rows"])

    df.withColumn("log_timestamp", current_timestamp()) \
      .write.format("delta") \
      .mode("append") \
      .saveAsTable("dbo.dq_invalid_summary_logs")

    print(f"[INVALID SUMMARY] {dataset} | invalid_rows = {count}")
