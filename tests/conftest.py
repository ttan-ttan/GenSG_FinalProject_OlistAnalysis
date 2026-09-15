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

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for all tests."""
    return SparkSession.builder.getOrCreate()
