# pylint: disable=E1101
"""
Creates a unified summary of DQ metrics across all datasets inside dbo schema.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


def generate_dq_summary():
    spark = SparkSession.builder.getOrCreate()

    # 1. Ensure summary table exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dbo.dq_summary (
            dataset STRING,
            silver_before BIGINT,
            silver_after BIGINT,
            dropped_rows BIGINT,
            invalid_rows BIGINT,
            dq_pass BOOLEAN,
            log_timestamp TIMESTAMP
        )
    """)

    # 2. Extract latest rowcount logs (corrected)
    rowcount_df = spark.sql("""
        WITH latest AS (
            SELECT
                dataset,
                stage,
                row_count,
                log_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY dataset, stage
                    ORDER BY log_timestamp DESC
                ) AS rn
            FROM dbo.dq_rowcount_logs
        )
        SELECT
            dataset,
            MAX(CASE WHEN stage = 'silver_before_clean' AND rn = 1 THEN row_count END) AS silver_before,
            MAX(CASE WHEN stage = 'silver_after_clean' AND rn = 1 THEN row_count END) AS silver_after
        FROM latest
        GROUP BY dataset
    """)

    # 3. Extract invalid rows summary
    invalid_df = spark.sql("""
        SELECT dataset, SUM(invalid_rows) AS invalid_rows
        FROM dbo.dq_invalid_summary_logs
        GROUP BY dataset
    """)

    # 4. Join and compute metrics
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
