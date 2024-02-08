import pytest
import pyspark.sql.functions as F
import package.schema_migration.apply_migrations as sut
from pyspark.sql import SparkSession
from unittest.mock import patch, Mock
from tests.schema_migration.schema_migration_helper import reset_migration_table
from package.constants.schema_migration import SchemaMigrationConstants
from package.schemas.schemas_config import schemas
from package.schema_migration.table_version import TableVersion

storage_account = "storage_account"
shared_storage_account = "shared_storage_account"


def _get_table_version(spark: SparkSession, schema: str, table: str) -> int:
    if not spark.catalog.tableExists(table, schema):
        return 0

    version = spark.sql(f"DESCRIBE HISTORY {schema}.{table}")
    current_version = version.orderBy(F.desc("version")).limit(1)
    return current_version.select("version").first()[0]


@patch.object(sut, sut._get_table_versions.__name__)
@patch.object(
    sut.SchemaMigrationConstants, "migration_scripts_folder_path", "tests.schema_migration.scripts"
)
def test_apply_uncommitted_migrations_applies_all(
    mock_table_versions: Mock, spark: SparkSession
) -> None:
    # Arrange
    reset_migration_table(spark)
    mock_table_versions.return_value = [TableVersion("test_schema.test_table", 0)]
    migrations = ["migration_step_1", "migration_step_2"]

    # Act
    sut.apply_uncommitted_migrations(migrations)

    # Assert
    assert spark.catalog.databaseExists("test_schema")
    assert spark.catalog.tableExists("test_table", "test_schema")

    actual = spark.table("test_schema.test_table").collect()[0]
    assert actual.column1 == "test1"
    assert actual.column2 == "test2"

    actual = spark.table(
        f"{SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}"
    ).collect()
    assert len(actual) == 2


@patch.object(sut, sut._get_table_versions.__name__)
@patch.object(
    sut.SchemaMigrationConstants, "migration_scripts_folder_path", "tests.schema_migration.scripts"
)
def test_apply_uncommitted_migrations_with_sql_file_with_error_should_rollback_table_and_raise_exception(
    mock_table_versions: Mock, spark: SparkSession
) -> None:
    # Test case:
    #   - Script 1 succeeds.
    #   - Script 2: Adds a column and change type on another, which would fail
    # The added column in script 2 should not be added because 2nd statement fails.
    # Arrange
    reset_migration_table(spark)
    mock_table_versions.return_value = [TableVersion("test_schema.test_table_fail", 0)]
    migrations = ["fail_migration_step_1", "fail_migration_step_2"]

    # Act
    with pytest.raises(Exception):
        sut.apply_uncommitted_migrations(migrations)

    # Assert
    assert spark.catalog.databaseExists("test_schema") is True
    assert spark.catalog.tableExists("test_table_fail", "test_schema") is True

    cols = spark.table("test_schema.test_table_fail").columns
    assert len(cols) == 2


@patch.object(sut, sut._insert_executed_sql_script.__name__)
@patch.object(sut, sut._get_table_versions.__name__)
@patch.object(
    sut.SchemaMigrationConstants, "migration_scripts_folder_path", "tests.schema_migration.scripts"
)
def test_apply_uncommitted_migrations_with_schema_migration_insert_fail_rollback_table(
    mock_table_version: Mock, mock_insert: Mock, spark: SparkSession
) -> None:
    # Arrange
    spark.sql("DROP TABLE IF EXISTS test_schema.test_table")
    mock_insert.side_effect = Exception("mocked error")
    mock_table_version.return_value = [TableVersion("test_schema.test_table", 0)]
    migrations = ["migration_test_version"]

    # Act
    with pytest.raises(Exception):
        sut.apply_uncommitted_migrations(migrations)

    # Assert
    history = spark.sql("DESCRIBE HISTORY test_schema.test_table")
    current_version = history.orderBy(F.desc("version")).limit(1)
    assert current_version.select("version").first()[0] == 2
    assert current_version.select("operation").first()[0] == "RESTORE"


@patch.object(sut, sut._get_table_versions.__name__)
@patch.object(
    sut.SchemaMigrationConstants, "migration_scripts_folder_path", "tests.schema_migration.scripts"
)
def test_apply_uncommitted_migrations_version_is_bumped(
    mock_table_version: Mock, spark: SparkSession
) -> None:
    # Arrange
    reset_migration_table(spark)
    current_version = _get_table_version(spark, "test_schema", "test_table")
    mock_table_version.return_value = [TableVersion("test_schema.test_table", current_version)]
    migrations = ["migration_test_version"]
    expected_version = current_version + 1

    # Act
    sut.apply_uncommitted_migrations(migrations)

    # Assert
    actual_version = _get_table_version(spark, "test_schema", "test_table")
    assert expected_version == actual_version


def test_insert_executed_sql_script(spark: SparkSession) -> None:
    # Arrange
    reset_migration_table(spark)
    migration_name = "test_name"

    # Act
    sut._insert_executed_sql_script(migration_name)

    # Assert
    actual = spark.table(
        f"{SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}"
    ).collect()
    assert len(actual) == 1
    assert actual[0].migration_name == migration_name


@patch.object(sut, sut._get_table_versions.__name__)
@patch.object(
    sut.SchemaMigrationConstants, "migration_scripts_folder_path", "tests.schema_migration.scripts"
)
def test_apply_uncommitted_migrations_with_table_containing_go_in_column_name(
    mock_table_version: Mock, spark: SparkSession
) -> None:
    # Arrange
    reset_migration_table(spark)
    mock_table_version.return_value = [TableVersion("test_schema.test_table", 0)]
    migrations = ["migration_test_go"]

    # Act
    sut.apply_uncommitted_migrations(migrations)

    # Assert
    assert spark.catalog.databaseExists("test_schema")
    assert spark.catalog.tableExists("test_table", "test_schema")


def test_get_table_versions_containing_all_tables(spark: SparkSession) -> None:
    # Arrange
    reset_migration_table(spark)
    location = test_get_table_versions_containing_all_tables.__name__

    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema.name} LOCATION '{location}'")
        for table in schema.tables:
            schema_df = spark.createDataFrame([], schema=table.schema)
            ddl = schema_df._jdf.schema().toDDL()
            spark.sql(
                f"CREATE TABLE {schema.name}.{table.name} ({ddl}) USING DELTA LOCATION '{location}/{schema.name}/{table.name}'"
            )

    # Act
    actual = sut._get_table_versions()

    # Assert
    for schema in schemas:
        for table in schema.tables:
            table_version = TableVersion(f"{schema.name}.{table.name}", 0)
            assert any(
                table_version.table_name == actual_table_version.table_name
                and table_version.version == actual_table_version.version
                for actual_table_version in actual
            )
