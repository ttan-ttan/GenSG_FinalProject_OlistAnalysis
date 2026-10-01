# pylint: disable=E1101
"""
Creates a unified summary of DQ metrics across all datasets.
"""

from pyspark.sql import SparkSession


def generate_dq_summary():
    spark = SparkSession.builder.getOrCreate()

    # Ensure summary table exists
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dq_summary (
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

    # Rowcount logs
    rowcount_df = spark.sql(
        """
        SELECT dataset,
               MAX(CASE WHEN stage = 'silver_before_clean' THEN row_count END) AS silver_before,
               MAX(CASE WHEN stage = 'silver_after_clean' THEN row_count END) AS silver_after
        FROM dq_rowcount_logs
        GROUP BY dataset
    """
    )

    # Invalid row logs
    invalid_df = spark.sql(
        """
        SELECT dataset, COUNT(*) AS invalid_rows
        FROM dq_invalid_logs
        GROUP BY dataset
    """
    )

    # Join both
    summary_df = (
        rowcount_df.alias("r")
        .join(invalid_df.alias("i"), "dataset", "left")
        .withColumn(
            "dropped_rows", rowcount_df.silver_before - rowcount_df.silver_after
        )
        .withColumn("dq_pass", (rowcount_df.silver_after > 0))
        .withColumn("log_timestamp", spark.sql("SELECT current_timestamp()").first()[0])
    )

    # Append summary
    summary_df.write.format("delta").mode("overwrite").saveAsTable("dq_summary")

    print("[DQ SUMMARY] Summary table updated.")
