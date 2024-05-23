﻿import pytest
import spark_sql_migrations.infrastructure.sql_file_executor as sut
import tests.builders.spark_sql_migrations_configuration_builder as spark_configuration_builder
from pyspark.sql import SparkSession
from spark_sql_migrations.models.spark_sql_migrations_configuration import (
    SparkSqlMigrationsConfiguration,
)
from spark_sql_migrations.container import create_and_configure_container


storage_account = "storage_account"
shared_storage_account = "shared_storage_account"
script_folder = "tests.test_scripts"


def _test_configuration() -> SparkSqlMigrationsConfiguration:
    configuration = spark_configuration_builder.build(
        migration_scripts_folder_path="tests.test_scripts",
        substitutions={
            "{bronze_location}": f"{storage_account}/bronze/",
            "{silver_location}": f"{storage_account}/silver/",
            "{gold_location}": f"{storage_account}/gold/",
            "{eloverblik_location}": f"{storage_account}/eloverblik",
            "{wholesale_location}": f"{shared_storage_account}/wholesale/",
        },
    )
    create_and_configure_container(configuration)

    return configuration


def test__execute__should_creates_schema(spark: SparkSession) -> None:
    # Arrange
    _test_configuration()
    migration_name = "create_schema"

    # Act
    sut.execute(migration_name, script_folder)

    # Assert
    assert spark.catalog.databaseExists("spark_catalog.test_schema")


def test__execute__when_multiple_queries__should_create_all_queries(
    spark: SparkSession,
) -> None:
    # Arrange
    _test_configuration()
    migration_name = "multiple_queries"

    # Act
    sut.execute(migration_name, script_folder)

    # Assert
    assert spark.catalog.databaseExists("spark_catalog.test_schema")
    assert spark.catalog.tableExists(f"spark_catalog.test_schema.test_table")


def test__execute__when_multiline_query__should_execute_query(
    spark: SparkSession,
) -> None:
    # Arrange
    _test_configuration()
    migration_name = "multiline_query"

    # Act
    sut.execute(migration_name, script_folder)

    # Assert
    assert spark.catalog.tableExists(f"spark_catalog.test_schema.test_table")


@pytest.mark.parametrize(
    "placeholder, expected_table, expected_storage_account",
    [
        pytest.param("{bronze_location}", "bronze", storage_account),
        pytest.param("{silver_location}", "silver", storage_account),
        pytest.param("{gold_location}", "gold", storage_account),
        pytest.param("{eloverblik_location}", "eloverblik", storage_account),
        pytest.param("{wholesale_location}", "wholesale", shared_storage_account),
    ],
)
def test__substitute_placeholders__should_replace_placeholders_in_query(
    placeholder: str,
    expected_table: str,
    expected_storage_account: str,
    spark: SparkSession,
) -> None:
    # Arrange
    _test_configuration()
    sql = f"CREATE SCHEMA IF NOT EXISTS spark_catalog.test_schema LOCATION {placeholder}"

    # Act
    query = sut._substitute_placeholders(sql)

    # Assert
    assert expected_table in query
    assert expected_storage_account in query
