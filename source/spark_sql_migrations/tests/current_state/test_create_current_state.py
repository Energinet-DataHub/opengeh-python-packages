import pytest
import spark_sql_migrations.current_state.create_current_state as sut

from pyspark.sql import SparkSession
from unittest.mock import Mock
from tests.helpers.test_schemas import schema_config
from tests.helpers.spark_helper import reset_spark_catalog


def test__get_schema_scripts__should_match_schema_config() -> None:
    # Arrange
    schemas = []
    for schema in schema_config:
        schemas.append(schema.name)

    # Act
    actual = sut._get_schema_scripts()

    # Assert
    assert len(actual) == len(schemas)


def test__get_table_scripts__should_match_schema_config() -> None:
    # Arrange
    tables = []
    for schema in schema_config:
        for table in schema.tables:
            tables.append(table.name)

    # Act
    actual = sut._get_table_scripts()

    # Assert
    assert len(actual) == len(tables)


def test__create_all_tables__should_create_all_tables(spark: SparkSession) -> None:
    # Arrange
    reset_spark_catalog(spark)

    # Act
    sut.create_all_tables()

    # Assert
    for schema in schema_config:
        for table in schema.tables:
            assert spark.catalog.tableExists(table.name, schema.name)


def test__create_all_tables__when_table_is_missing__it_should_create_the_missing_tables(
    spark: SparkSession,
) -> None:
    # Arrange
    reset_spark_catalog(spark)

    sut.create_all_tables()
    spark.sql("DROP TABLE test_schema.test_table_2")

    # Act
    sut.create_all_tables()

    # Assert
    for schema in schema_config:
        for table in schema.tables:
            assert spark.catalog.tableExists(table.name, schema.name)


def test__create_all_table__when_an_error_occurs__it_should_throw_an_exception(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    reset_spark_catalog(spark)
    mocker.patch.object(
        sut.sql_file_executor,
        sut.sql_file_executor.execute.__name__,
        side_effect=raise_exception,
    )

    # Act
    with pytest.raises(Exception):
        sut.create_all_tables()


def path_helper(storage_account: str, container: str, folder: str = "") -> str:
    return container


def raise_exception():
    raise Exception("Test exception")
