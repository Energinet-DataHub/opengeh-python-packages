"""Utility functions for ETL testing.

This module provides utility functions to aid in feature testing.
"""

from testcommon.etl.read_csv import read_csv
from testcommon.etl.get_then_names import get_then_names
from testcommon.etl.TestCases import TestCases, TestCase

# We are using the assertDataFrameEqual function from PySpark.
# This function has A LOT of dependency issues. These have been resolved in
# the setup.py file. So we can import the function here without any issues.
from pyspark.testing.utils import assertDataFrameEqual as assert_dataframes
from pyspark.testing.utils import assertSchemaEqual as assert_schemas

__all__ = [
    read_csv.__name__,
    assert_dataframes.__name__,
    get_then_names.__name__,
    assert_schemas.__name__,
    TestCases.__name__,
    TestCase.__name__,
]
