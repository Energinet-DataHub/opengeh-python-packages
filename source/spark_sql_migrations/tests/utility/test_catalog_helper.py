import pytest
from unittest.mock import Mock
from pyspark.sql import SparkSession
from spark_sql_migrations.utility.catalog_helper import is_unity_catalog


@pytest.mark.parametrize(
    "count, unity_enabled, expected",
    [
        (1, True, True),
        (0, True, False),
        (1, False, False),
        (0, False, False)
    ]
)
def test__is_a_unity_catalog(spark: SparkSession, count: int, unity_enabled: bool, expected: bool):
    # Arrange
    spark.catalog.databaseExists = lambda _: unity_enabled
    spark.sql = lambda _: spark.createDataFrame([(count,)], ["count"])

    # Act
    result = is_unity_catalog(spark, "test_catalog")

    # Assert
    assert result is expected
