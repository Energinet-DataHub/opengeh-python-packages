import pytest
from unittest.mock import Mock
from pyspark.sql import SparkSession
from spark_sql_migrations.utility.catalog_helper import is_unity_catalog


@pytest.mark.parametrize(
    "count, expected",
    [
        (1, True),
        (0, False)
    ]
)
def test__is_a_unity_catalog(spark: SparkSession, count: int, expected: bool):
    # Arrange
    mock_result = spark.createDataFrame([(count,)], ["count"])
    spark.sql = lambda _: mock_result

    # Act
    result = is_unity_catalog(spark, "test_catalog")

    # Assert
    assert result is expected
