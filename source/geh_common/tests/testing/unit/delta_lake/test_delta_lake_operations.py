import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from geh_common.testing.delta_lake.delta_lake_operations import (
    create_database,
    create_table,
)

DEFAULT_DATABASE_NAME = "test_db"
DEFAULT_TABLE_NAME = "test_table"
DEFAULT_LOCATION = "/tmp/test_table"
DEFAULT_SCHEMA = StructType([StructField("name", StringType(), True)])


def test_create_database__creates_database(spark: SparkSession):
    # Arrange
    database_name = "test_db"

    # Act
    create_database(spark, database_name)

    # Assert
    databases = [db.name for db in spark.catalog.listDatabases()]
    assert database_name in databases


def test_create_database__when_already_exists__does_not_create(spark: SparkSession):
    # Arrange
    database_name = "existing_db"
    create_database(spark, database_name)  # Create the database initially

    # Act
    create_database(spark, database_name)  # Try to create the same database again

    # Assert
    databases = [db.name for db in spark.catalog.listDatabases()]
    assert databases.count(database_name) == 1  # Ensure the database is not duplicated


def test_create_table__creates_table(spark: SparkSession):
    # Arrange
    create_database(spark, DEFAULT_DATABASE_NAME)

    # Act
    create_table(
        spark,
        DEFAULT_DATABASE_NAME,
        DEFAULT_TABLE_NAME,
        DEFAULT_SCHEMA,
    )

    # Assert
    tables = [table.name for table in spark.catalog.listTables(DEFAULT_DATABASE_NAME)]
    assert DEFAULT_TABLE_NAME in tables


def test_create_table__when_already_exists__does_not_create(spark: SparkSession):
    # Arrange
    create_database(spark, DEFAULT_DATABASE_NAME)
    create_table(
        spark,
        DEFAULT_DATABASE_NAME,
        DEFAULT_TABLE_NAME,
        DEFAULT_SCHEMA,
    )

    # Act
    with pytest.raises(Exception):
        create_table(
            spark,
            DEFAULT_DATABASE_NAME,
            DEFAULT_TABLE_NAME,
            DEFAULT_SCHEMA,
        )  # Try to create the same table again

    # Assert
    tables = [table.name for table in spark.catalog.listTables(DEFAULT_DATABASE_NAME)]
    assert tables.count(DEFAULT_TABLE_NAME) == 1  # Ensure the table is not duplicated


def test_create_table__schema_is_as_expected(spark: SparkSession):
    # Arrange
    create_database(spark, DEFAULT_DATABASE_NAME)
    expected_schema = DEFAULT_SCHEMA

    # Act
    create_table(
        spark,
        DEFAULT_DATABASE_NAME,
        DEFAULT_TABLE_NAME,
        expected_schema,
        DEFAULT_LOCATION,
    )

    # Assert
    actual_schema = spark.read.table(f"{DEFAULT_DATABASE_NAME}.{DEFAULT_TABLE_NAME}").schema
    assert actual_schema == expected_schema, f"Expected schema: {expected_schema}, but got: {actual_schema}"
