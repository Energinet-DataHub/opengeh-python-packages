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
def test__is_a_unity_catalog(spark: SparkSession, mocker: Mock, count: int, unity_enabled: bool, expected: bool):
    # Arrange

    mocker.patch.object(spark.catalog, "databaseExists", return_value=unity_enabled)
    mocker.patch.object(spark, "sql", return_value=spark.createDataFrame([(count,)], ["count"]))

    # Act
    result = is_unity_catalog(spark, "test_catalog")

    # Assert
    assert result is expected
