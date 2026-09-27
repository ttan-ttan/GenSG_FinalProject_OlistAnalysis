"""
validation_reviews.py

This file does NOT change any data. It only CHECKS data and reports
problems. Think of cleaning_reviews.py as "fix it" and this file as
"check it" - keeping them separate makes each one easier to test and
reason about on its own.

You can run this on the RAW data (to see how messy it is) or on the
CLEANED data (as a final safety check that cleaning worked properly).
"""

from __future__ import annotations

from dataclasses import (  # dataclass = an easy way to define a simple "data holder" class
    dataclass,
    field,
)

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# columns that MUST exist for validation to even be possible
REQUIRED_COLUMNS: list[str] = [
    "review_id",
    "order_id",
    "review_score",
    "review_creation_date",
    "review_answer_timestamp",
]

# business rule: review_score must be a whole number from 1 to 5
MIN_SCORE = 1
MAX_SCORE = 5


@dataclass
class ValidationResult:
    """
    A simple container that holds the outcome of running all our checks.

    passed   -> True if there were no hard errors
    row_count -> how many rows were checked
    errors   -> list of problems that make the data UNUSABLE
    warnings -> list of problems that are worth knowing about but don't
                block usage (e.g. duplicate review_id, which cleaning fixes)
    metrics  -> raw numbers behind each check, useful for logging/debugging
    """

    passed: bool
    row_count: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)

    def raise_if_failed(self) -> None:
        """Convenience method: call this if you want validation failure to
        stop your pipeline immediately with a Python exception."""
        if not self.passed:
            raise ValueError(
                "order_reviews validation failed: " + "; ".join(self.errors)
            )


def _check_required_columns(df: DataFrame, errors: list[str]) -> bool:
    """Look for any column we need but don't have. Returns False if
    something's missing (and records the problem in `errors`)."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"missing required column(s): {missing}")
        return False
    return True


def validate_order_reviews(df: DataFrame) -> ValidationResult:
    """
    Runs every check we care about on the given DataFrame and returns a
    single ValidationResult summarising everything.
    """
    errors: list[str] = []
    warnings: list[str] = []
    metrics: dict = {}

    row_count = df.count()
    metrics["row_count"] = row_count

    # if columns are missing, there's no point running the rest of the
    # checks - Spark would just crash on a column that doesn't exist
    if not _check_required_columns(df, errors):
        return ValidationResult(
            passed=False,
            row_count=row_count,
            errors=errors,
            warnings=warnings,
            metrics=metrics,
        )

    if row_count == 0:
        errors.append("DataFrame is empty")

    # --- CHECK 1: are any of our key columns null? ------------------------
    # .filter(...isNull()).count() = "count how many rows have a null here"
    null_review_id = df.filter(F.col("review_id").isNull()).count()
    null_order_id = df.filter(F.col("order_id").isNull()).count()
    null_score = df.filter(F.col("review_score").isNull()).count()
    null_creation = df.filter(F.col("review_creation_date").isNull()).count()
    null_answer = df.filter(F.col("review_answer_timestamp").isNull()).count()

    # save these numbers so anyone reading the result can see the raw counts
    metrics.update(
        {
            "null_review_id": null_review_id,
            "null_order_id": null_order_id,
            "null_review_score": null_score,
            "null_review_creation_date": null_creation,
            "null_review_answer_timestamp": null_answer,
        }
    )

    # turn any non-zero count into a human-readable error message
    if null_review_id:
        errors.append(f"{null_review_id} row(s) with null review_id")
    if null_order_id:
        errors.append(f"{null_order_id} row(s) with null order_id")
    if null_score:
        errors.append(f"{null_score} row(s) with null review_score")
    if null_creation:
        errors.append(f"{null_creation} row(s) with null review_creation_date")
    if null_answer:
        errors.append(f"{null_answer} row(s) with null review_answer_timestamp")

    # --- CHECK 2: is review_score always between 1 and 5? -----------------
    out_of_range = df.filter(
        F.col("review_score").isNotNull()
        & ~F.col("review_score").between(MIN_SCORE, MAX_SCORE)
    ).count()
    metrics["review_score_out_of_range"] = out_of_range
    if out_of_range:
        errors.append(
            f"{out_of_range} row(s) with review_score outside [{MIN_SCORE}, {MAX_SCORE}]"
        )

    # --- CHECK 3: was any review "answered" before it was even created? ---
    # this would mean bad/corrupted data, so we treat it as an error
    reversed_timestamps = df.filter(
        F.col("review_creation_date").isNotNull()
        & F.col("review_answer_timestamp").isNotNull()
        & (F.col("review_answer_timestamp") < F.col("review_creation_date"))
    ).count()
    metrics["review_answer_before_creation"] = reversed_timestamps
    if reversed_timestamps:
        errors.append(
            f"{reversed_timestamps} row(s) where review_answer_timestamp is "
            "earlier than review_creation_date"
        )

    # --- CHECK 4: does review_id repeat? (warning, not an error) ----------
    # groupBy + count() tells us how many times each review_id appears.
    # We then count how many review_id GROUPS have more than 1 row.
    duplicate_review_ids = (
        df.filter(F.col("review_id").isNotNull())
        .groupBy("review_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    metrics["duplicate_review_id_groups"] = duplicate_review_ids
    if duplicate_review_ids:
        # this is only a WARNING, not an error, because cleaning_reviews.py
        # already knows how to fix this (keeps the newest one)
        warnings.append(
            f"{duplicate_review_ids} review_id value(s) appear more than once - "
            "run clean_order_reviews to de-duplicate before downstream use"
        )

    # passed = True only if we found zero errors (warnings are OK)
    passed = len(errors) == 0
    return ValidationResult(
        passed=passed,
        row_count=row_count,
        errors=errors,
        warnings=warnings,
        metrics=metrics,
    )


def quarantine_invalid_rows(df: DataFrame) -> DataFrame:
    """
    Instead of just counting problems, this function actually RETURNS the
    bad rows, each tagged with a `validation_reason` column explaining why
    it's bad. Useful if you want to eyeball the actual broken records.

    Note: a row that breaks more than one rule will show up more than
    once (once per rule it breaks).
    """
    # each entry is a (condition, reason_text) pair
    reasons = []

    if "review_id" in df.columns:
        reasons.append((F.col("review_id").isNull(), "null review_id"))
    if "order_id" in df.columns:
        reasons.append((F.col("order_id").isNull(), "null order_id"))
    if "review_score" in df.columns:
        reasons.append(
            (
                F.col("review_score").isNull()
                | ~F.col("review_score").between(MIN_SCORE, MAX_SCORE),
                "invalid review_score",
            )
        )
    if "review_creation_date" in df.columns:
        reasons.append(
            (F.col("review_creation_date").isNull(), "null review_creation_date")
        )
    if "review_answer_timestamp" in df.columns:
        reasons.append(
            (F.col("review_answer_timestamp").isNull(), "null review_answer_timestamp")
        )
    if "review_creation_date" in df.columns and "review_answer_timestamp" in df.columns:
        reasons.append(
            (
                F.col("review_creation_date").isNotNull()
                & F.col("review_answer_timestamp").isNotNull()
                & (F.col("review_answer_timestamp") < F.col("review_creation_date")),
                "review_answer_timestamp before review_creation_date",
            )
        )

    # start with nothing, then keep adding the matching rows for each rule,
    # tagging each batch with its own reason
    quarantined = None
    for condition, reason in reasons:
        tagged = df.filter(condition).withColumn("validation_reason", F.lit(reason))
        # unionByName "stacks" two DataFrames together (like appending rows)
        quarantined = tagged if quarantined is None else quarantined.unionByName(tagged)

    # if there were no rules to check (shouldn't normally happen), return
    # an empty DataFrame with the right shape instead of None
    if quarantined is None:
        return df.limit(0).withColumn("validation_reason", F.lit(""))

    return quarantined
