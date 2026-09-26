# pylint: disable=no-member

"""
Purpose:
    Shared pytest fixtures for the entire test suite.
    Pytest automatically loads this file — no import required.

Notes for team:
    - DO NOT delete this file.
    - DO NOT import it manually.
    - Pytest discovers it automatically.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Use the same interpreter for the Spark driver and worker processes.
# This prevents PYTHON_VERSION_MISMATCH when pytest is launched from a
# Python environment different from the one Spark tries to use by default.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[*]").getOrCreate()
    yield session
    session.stop()
