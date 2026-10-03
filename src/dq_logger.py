# pylint: disable=E1101
"""
Centralized DQ logging script for Fabric pipeline.
"""

from pyspark.sql import SparkSession


def log(stage, dataset, count):
    spark = SparkSession.builder.getOrCreate()
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dq_rowcount_logs (
            dataset STRING,
            stage STRING,
            row_count BIGINT,
            log_timestamp TIMESTAMP
        )
    """
    )

    # Use parameterized PySpark DataFrame write to prevent SQL syntax errors
    data = [(dataset, stage, int(count))]
    df = spark.createDataFrame(data, ["dataset", "stage", "row_count"])
    df.selectExpr("dataset", "stage", "row_count", "current_timestamp() as log_timestamp") \
      .write.format("delta").mode("append").saveAsTable("dq_rowcount_logs")

    print(f"[ROWCOUNT] {dataset} | {stage} | {count}")


def log_invalid(dataset, temp_view_name="all_invalid"):
    spark = SparkSession.builder.getOrCreate()

    # Check if the temporary view exists in Spark session
    if not spark.catalog.tableExists(temp_view_name):
        print(
            f"[WARNING] Temp view '{temp_view_name}' not found. Logging 0 invalid rows.")
        count = 0
    else:
        count = spark.table(temp_view_name).count()

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dq_invalid_logs (
            dataset STRING,
            invalid_rows BIGINT,
            log_timestamp TIMESTAMP
        )
    """
    )

    data = [(dataset, int(count))]
    df = spark.createDataFrame(data, ["dataset", "invalid_rows"])
    df.selectExpr("dataset", "invalid_rows", "current_timestamp() as log_timestamp") \
      .write.format("delta").mode("append").saveAsTable("dq_invalid_logs")

    print(f"[INVALID] {dataset} | invalid_rows = {count}")
