﻿import pytest
import tests.builders.spark_sql_migrations_configuration_builder as spark_sql_migrations_configuration_builder
import spark_sql_migrations.current_state.create_current_state as sut
from pyspark.sql import SparkSession
from unittest.mock import Mock
from tests.helpers.test_schemas import schema_config
from tests.helpers.spark_helper import reset_spark_catalog
from spark_sql_migrations.container import create_and_configure_container


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


def test__get_view_scripts__should_be_sorted_by_name() -> None:
    # Arrange
    expected = [
        "create__view_1",
        "create_view_2",
        "create_view_3",
    ]

    # Act
    actual = sut._get_view_scripts()

    # Assert
    assert len(actual) == len(expected)
    assert actual == expected


def test__create_all_tables__should_create_all_tables_and_views(
    spark: SparkSession,
) -> None:
    # Arrange
    reset_spark_catalog(spark)
    # Act
    sut.create_all_tables()

    # Assert
    for schema in schema_config:
        for table in schema.tables:
            assert spark.catalog.tableExists(f"spark_catalog.{schema.name}.{table.name}")

        for view in schema.views:
            assert spark.catalog.tableExists(f"spark_catalog.{schema.name}.{view.name}")


def test__create_all_tables__when_table_is_missing__it_should_create_the_missing_tables(
    spark: SparkSession,
) -> None:
    # Arrange
    reset_spark_catalog(spark)

    sut.create_all_tables()
    spark.sql("DROP TABLE spark_catalog.test_schema.test_table_2")

    # Act
    sut.create_all_tables()

    # Assert
    for schema in schema_config:
        for table in schema.tables:
            assert spark.catalog.tableExists(f"spark_catalog.{schema.name}.{table.name}")


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
