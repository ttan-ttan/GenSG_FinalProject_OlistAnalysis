"""
validation_payments.py

PySpark data-quality validation for a cleaned Olist order_payments DataFrame.

Importable in a Fabric notebook via:
    import sys
    sys.path.append('/lakehouse/default/Files/code/src')
    from validation_payments import validate_order_payments, ValidationError
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]

VALID_PAYMENT_TYPES = ["boleto", "credit_card", "debit_card", "not_defined", "voucher"]

ORDER_ID_LENGTH = 32  # Olist order_id is a fixed-length hex string


class ValidationError(Exception):
    """Raised when the DataFrame fails a critical check and strict=True."""


def _flag(condition) -> F.Column:
    """1 when the condition is true, 0 when it is false OR null."""
    return F.when(condition, F.lit(1)).otherwise(F.lit(0))


def _count_where(condition) -> F.Column:
    """
    Sum of the flag, coalesced to 0.

    F.sum over an empty DataFrame returns null, not 0, so without the coalesce
    an empty input would blow up on int(None).
    """
    return F.coalesce(F.sum(_flag(condition)), F.lit(0))


def validate_order_payments(df: DataFrame, strict: bool = True) -> dict:
    """
    Validate a cleaned order_payments Spark DataFrame.

    Critical checks (added to "errors"; raise ValidationError if strict=True):
      - all required columns present
      - no nulls in required columns
      - order_id is exactly 32 characters
      - payment_sequential >= 1
      - payment_value >= 0 (no negative payments)
      - payment_installments >= 0
      - payment_type is one of the known categories
      - no duplicate (order_id, payment_sequential) pairs

    Warning checks (added to "warnings"; never raise):
      - payment_value == 0 (legitimate for some voucher / not_defined rows)
      - payment_installments == 0

    All the row-level predicates are counted in a single aggregation pass so
    Spark scans the data once rather than once per rule.

    Returns:
        {"passed": bool, "errors": [...], "warnings": [...], "row_count": int}
    """
    errors: list[str] = []
    warnings: list[str] = []

    # check required cols exist first, bail out early if not
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        errors.append(f"Missing required column(s): {sorted(missing)}")
        report = {
            "passed": False,
            "errors": errors,
            "warnings": warnings,
            "row_count": None,
        }
        if strict:
            raise ValidationError("; ".join(errors))
        return report

    df = df.cache()  # reused across multiple aggs below, so cache it

    try:
        # one null-count agg per required column
        null_aggs = [
            _count_where(F.col(c).isNull()).alias(f"null__{c}")
            for c in REQUIRED_COLUMNS
        ]

        # all other row-level checks, computed in the same single pass
        other_aggs = [
            F.count(F.lit(1)).alias("row_count"),
            _count_where(
                F.col("order_id").isNotNull()
                & (F.length(F.col("order_id")) != F.lit(ORDER_ID_LENGTH))
            ).alias("bad_id_len"),
            _count_where(F.col("payment_sequential") < F.lit(1)).alias("bad_seq"),
            _count_where(F.col("payment_value") < F.lit(0)).alias("negative_value"),
            _count_where(F.col("payment_installments") < F.lit(0)).alias(
                "negative_installments"
            ),
            _count_where(
                F.col("payment_type").isNotNull()
                & ~F.col("payment_type").isin(VALID_PAYMENT_TYPES)
            ).alias("bad_type"),
            _count_where(F.col("payment_value") == F.lit(0)).alias("zero_value"),
            _count_where(F.col("payment_installments") == F.lit(0)).alias(
                "zero_installments"
            ),
        ]

        # run everything in ONE aggregation call (single scan of the data)
        stats = df.agg(*(null_aggs + other_aggs)).collect()[0].asDict()

        # duplicate (order_id, payment_sequential) pairs need a separate groupBy pass
        dup_row = (
            df.groupBy("order_id", "payment_sequential")
            .agg(F.count(F.lit(1)).alias("n"))
            .filter(F.col("n") > F.lit(1))
            .agg(F.coalesce(F.sum("n"), F.lit(0)).alias("dup_rows"))
            .collect()[0]
        )
        dup_rows = int(dup_row["dup_rows"])

        row_count = int(stats["row_count"])

        # turn each stat count into a human-readable error message
        for col_name in REQUIRED_COLUMNS:
            count = int(stats[f"null__{col_name}"])
            if count > 0:
                errors.append(f"Column '{col_name}' has {count} null value(s)")

        if int(stats["bad_id_len"]) > 0:
            errors.append(
                f"{int(stats['bad_id_len'])} row(s) have order_id not equal to "
                f"{ORDER_ID_LENGTH} characters"
            )

        if int(stats["bad_seq"]) > 0:
            errors.append(f"{int(stats['bad_seq'])} row(s) have payment_sequential < 1")

        if int(stats["negative_value"]) > 0:
            errors.append(
                f"{int(stats['negative_value'])} row(s) have negative payment_value"
            )

        if int(stats["negative_installments"]) > 0:
            errors.append(
                f"{int(stats['negative_installments'])} row(s) have negative payment_installments"
            )

        if int(stats["bad_type"]) > 0:
            # only re-scan the data here to find *which* bad types exist, for the error message
            found = sorted(
                r["payment_type"]
                for r in (
                    df.filter(
                        F.col("payment_type").isNotNull()
                        & ~F.col("payment_type").isin(VALID_PAYMENT_TYPES)
                    )
                    .select("payment_type")
                    .distinct()
                    .collect()
                )
            )
            errors.append(
                f"{int(stats['bad_type'])} row(s) have payment_type outside "
                f"{sorted(VALID_PAYMENT_TYPES)}: {found}"
            )

        if dup_rows > 0:
            errors.append(
                f"{dup_rows} row(s) share a duplicate (order_id, payment_sequential) pair"
            )

        # warnings — don't fail validation, just flag
        if int(stats["zero_value"]) > 0:
            warnings.append(
                f"{int(stats['zero_value'])} row(s) have payment_value == 0"
            )

        if int(stats["zero_installments"]) > 0:
            warnings.append(
                f"{int(stats['zero_installments'])} row(s) have payment_installments == 0"
            )
    finally:
        df.unpersist()  # always release cache, even if something above raises

    passed = len(errors) == 0
    report = {
        "passed": passed,
        "errors": errors,
        "warnings": warnings,
        "row_count": row_count,
    }

    if strict and not passed:
        raise ValidationError("; ".join(errors))

    return report  # was "reportx" in your version — typo, fixed here
