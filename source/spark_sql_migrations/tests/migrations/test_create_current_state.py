import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch, Mock
from package.schemas.schemas_config import schemas
import tests.schema_migration.schema_migration_helper as schema_migration_helper
import package.schema_migration.create_current_state as sut
import package.schemas.schemas_config as schema_config


def test_get_schema_scripts_matches_schema_config() -> None:
    # Arrange
    schemas = []
    for schema in schema_config.schemas:
        schemas.append(schema.name)

    # Act
    actual = sut._get_schema_scripts()

    # Assert
    assert len(actual) == len(schemas)


def test_get_table_scripts_matches_schema_config() -> None:
    # Arrange
    tables = []
    for schema in schema_config.schemas:
        for table in schema.tables:
            tables.append(table.name)

    # Act
    actual = sut._get_table_scripts()

    # Assert
    assert len(actual) == len(tables)


@patch.object(
    sut.sql_file_executor.path_helper,
    sut.sql_file_executor.path_helper.get_storage_base_path.__name__,
)
def test_create_all_tables_creates_all_tables(mock_path_helper: Mock, spark: SparkSession) -> None:
    # Arrange
    schema_migration_helper.reset_schema_configured_tables(spark)
    mock_path_helper.side_effect = path_helper

    # Act
    sut.create_all_tables()

    # Assert
    for schema in schemas:
        for table in schema.tables:
            assert spark.catalog.tableExists(table.name, schema.name)


@patch.object(
    sut.sql_file_executor.path_helper,
    sut.sql_file_executor.path_helper.get_storage_base_path.__name__,
)
def test_create_all_tables_when_table_is_missing_it_should_create_the_missing_tables(
    mock_path_helper: Mock, spark: SparkSession
) -> None:
    # Arrange
    mock_path_helper.side_effect = path_helper
    schema_migration_helper.reset_schema_configured_tables(spark)

    sut.create_all_tables()
    spark.sql("DROP TABLE bronze.time_series")
    spark.sql("DROP TABLE wholesale.time_series_points")

    # Act
    sut.create_all_tables()

    # Assert
    for schema in schemas:
        for table in schema.tables:
            assert spark.catalog.tableExists(table.name, schema.name)


@patch.object(sut.sql_file_executor, sut.sql_file_executor.execute.__name__)
def test_create_all_table_when_an_error_occurs_it_should_throw_an_exception(
    mock_sql_file_executor: Mock, spark: SparkSession
) -> None:
    # Arrange
    schema_migration_helper.reset_schema_configured_tables(spark)
    mock_sql_file_executor.side_effect = raise_exception

    # Act
    with pytest.raises(Exception):
        sut.create_all_tables()


def path_helper(storage_account: str, container: str, folder: str = "") -> str:
    return container


def raise_exception():
    raise Exception("Test exception")
