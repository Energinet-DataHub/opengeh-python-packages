import pytest
from pyspark.sql import SparkSession
from spark_sql_migrations.utility.catalog_helper import is_unity_catalog


@pytest.mark.parametrize(
    "catalog_name, expected",
    [
        ("spark_catalog", False),
        ("hive_metastore", False),
        ("a_unity_catalog", True),
    ]
)
def test__is_a_unity_catalog(spark: SparkSession, catalog_name: str, expected: bool):
    # Arrange

    # Act
    result = is_unity_catalog(spark, catalog_name)

    # Assert
    assert result is expected
