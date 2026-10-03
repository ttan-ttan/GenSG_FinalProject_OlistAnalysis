# pylint: disable=E1101
"""
Creates a unified summary of DQ metrics across all datasets inside dbo schema.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


def generate_dq_summary():
    spark = SparkSession.builder.getOrCreate()

    # 1. Ensure summary table exists in dbo schema
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dbo.dq_summary (
            dataset STRING,
            silver_before BIGINT,
            silver_after BIGINT,
            dropped_rows BIGINT,
            invalid_rows BIGINT,
            dq_pass BOOLEAN,
            log_timestamp TIMESTAMP
        )
        """
    )

    # 2. Extract Rowcount logs
    rowcount_df = spark.sql(
        """
        SELECT dataset,
               MAX(CASE WHEN stage = 'silver_before_clean' THEN row_count END) AS silver_before,
               MAX(CASE WHEN stage = 'silver_after_clean' THEN row_count END) AS silver_after
        FROM dbo.dq_rowcount_logs
        GROUP BY dataset
        """
    )

    # 3. Extract Invalid summary row logs (uses SUM to aggregate multiple runs)
    invalid_df = spark.sql(
        """
        SELECT dataset, SUM(invalid_rows) AS invalid_rows
        FROM dbo.dq_invalid_summary_logs
        GROUP BY dataset
        """
    )

    # 4. Join and calculate metrics safely
    summary_df = (
        rowcount_df.alias("r")
        .join(invalid_df.alias("i"), "dataset", "left")
        .withColumn("dropped_rows", col("silver_before") - col("silver_after"))
        .withColumn("dq_pass", col("silver_after") > 0)
        .withColumn("log_timestamp", current_timestamp())
    )

    # 5. Overwrite summary table
    summary_df.write.format("delta").mode(
        "overwrite").saveAsTable("dbo.dq_summary")

    print("[DQ SUMMARY] Summary table dbo.dq_summary updated.")
