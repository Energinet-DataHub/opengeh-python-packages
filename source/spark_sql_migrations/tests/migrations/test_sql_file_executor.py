import pytest
import package.schema_migration.sql_file_executor as sut
from pyspark.sql import SparkSession
from unittest.mock import patch, Mock

storage_account = "storage_account"
shared_storage_account = "shared_storage_account"
script_folder = "tests.schema_migration.scripts"


@patch.object(sut.path_helper, sut.path_helper.get_storage_base_path.__name__)
def test_execute_creates_schema(mock_location: Mock, spark: SparkSession) -> None:
    # Arrange
    mock_location.return_value = "some_location"
    migration_name = "create_schema"

    # Act
    sut.execute(script_folder, migration_name)

    # Assert
    assert spark.catalog.databaseExists("test_schema")


@patch.object(sut.path_helper, sut.path_helper.get_storage_base_path.__name__)
def test_execute_with_multiple_queries(mock_location: Mock, spark: SparkSession) -> None:
    # Arrange
    mock_location.return_value = "some_location"
    migration_name = "multiple_queries"

    # Act
    sut.execute(script_folder, migration_name)

    # Assert
    assert spark.catalog.databaseExists("test_schema")
    assert spark.catalog.tableExists("test_table", "test_schema")


@patch.object(sut.path_helper, sut.path_helper.get_storage_base_path.__name__)
def test_execute_with_multiline_query(mock_location: Mock, spark: SparkSession) -> None:
    # Arrange
    mock_location.return_value = "some_location"
    migration_name = "multiline_query"

    # Act
    sut.execute(script_folder, migration_name)

    # Assert
    assert spark.catalog.tableExists("test_table", "test_schema")


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
def test__substitute_placeholders_replace_placeholders(
    placeholder: str, expected_table: str, expected_storage_account: str, spark: SparkSession
) -> None:
    # Arrange
    sql = f"CREATE SCHEMA IF NOT EXISTS test_schema LOCATION {placeholder}"

    # Act
    query = sut._substitute_placeholders(sql)

    # Assert
    assert expected_table in query
    assert expected_storage_account in query


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
def test__substitutions_returns_substitutions(
    placeholder: str, expected_table: str, expected_storage_account: str, spark: SparkSession
) -> None:
    # Act
    substitutions = sut._substitutions(
        storage_account=storage_account, shared_storage_account=shared_storage_account
    )

    # Assert
    assert expected_table in substitutions[placeholder]
    assert expected_storage_account in substitutions[placeholder]
