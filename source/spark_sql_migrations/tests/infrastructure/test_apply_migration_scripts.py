import pytest
import pyspark.sql.functions as F
import tests.helpers.table_helper as table_helper
import spark_sql_migrations.infrastructure.apply_migration_scripts as sut
from unittest.mock import Mock
from pyspark.sql import SparkSession
from tests.helpers.spark_helper import reset_spark_catalog
from spark_sql_migrations.models.table_version import TableVersion
from spark_sql_migrations.container import create_and_configure_container
import tests.helpers.test_schemas as test_schemas
from spark_sql_migrations.schemas.migrations_schema import schema_migration_schema
import tests.builders.spark_sql_migrations_configuration_builder as configuration_builder
from spark_sql_migrations.models.spark_sql_migrations_configuration import (
    SparkSqlMigrationsConfiguration,
)


shared_storage_account = "shared_storage_account"
storage_account = "storage_account"


def _test_configuration(
    spark: SparkSession, rollback_on_failure: bool = False
) -> SparkSqlMigrationsConfiguration:
    configuration = configuration_builder.build(
        migration_scripts_folder_path="tests.test_scripts",
        rollback_on_failure=rollback_on_failure,
    )
    create_and_configure_container(configuration)

    table_helper.create_schema_and_table(
        spark,
        configuration.catalog_name,
        configuration.migration_schema_name,
        configuration.migration_table_name,
        schema_migration_schema,
    )

    return configuration


def test__apply_uncommitted_migrations__it_should_apply_all_scripts(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    reset_spark_catalog(spark)
    mocker.patch.object(
        sut,
        sut._get_table_versions.__name__,
        return_value=[TableVersion("test_schema.test_table", 0)],
    )

    migrations = ["migration_step_1", "migration_step_2"]

    configuration = _test_configuration(spark)

    # Act
    sut.apply_migration_scripts(migrations)

    # Assert
    assert spark.catalog.databaseExists("spark_catalog.test_schema")
    assert spark.catalog.tableExists("spark_catalog.test_schema.test_table")

    actual = spark.table("spark_catalog.test_schema.test_table").collect()[0]
    assert actual.column1 == "test1"
    assert actual.column2 == "test2"

    actual = spark.table(
        f"{configuration.catalog_name}.{configuration.migration_schema_name}.{configuration.migration_table_name}"
    ).collect()
    assert len(actual) == 2


def test__apply_uncommitted_migrations__when_sql_file_with_error__it_should_rollback_table_and_raise_exception(
    mocker: Mock, spark: SparkSession
) -> None:
    # Test case:
    #   - Script 1 succeeds.
    #   - Script 2: Adds a column and change type on another, which would fail
    # The added column in script 2 should not be added because 2nd statement fails.
    # Arrange
    reset_spark_catalog(spark)

    mocker.patch.object(
        sut,
        sut._get_table_versions.__name__,
        return_value=[TableVersion("test_schema.test_table_fail", 0)],
    )
    migrations = ["fail_migration_step_1", "fail_migration_step_2"]
    _test_configuration(spark, rollback_on_failure=True)

    # Act
    with pytest.raises(Exception):
        sut.apply_migration_scripts(migrations)

    # Assert
    assert spark.catalog.databaseExists("spark_catalog.test_schema") is True
    assert (
        spark.catalog.tableExists("spark_catalog.test_schema.test_table_fail") is True
    )

    cols = spark.table("spark_catalog.test_schema.test_table_fail").columns
    assert len(cols) == 2


def test__apply_uncommitted_migrations__when_schema_migration_insert_fails__it_should_rollback_table(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    _test_configuration(spark, rollback_on_failure=True)
    test_schemas.create_test_tables(spark)
    mocker.patch.object(
        sut,
        sut._insert_executed_sql_script.__name__,
        side_effect=Exception("mocked error"),
    )

    migrations = ["migration_test_restore"]

    expected_version = table_helper.get_current_table_version(
        spark, "test_schema", "test_table"
    )

    # Act
    with pytest.raises(Exception):
        sut.apply_migration_scripts(migrations)

    # Assert
    history = spark.sql("DESCRIBE HISTORY spark_catalog.test_schema.test_table")
    current_version = history.orderBy(F.desc("version")).limit(1)
    assert current_version.select("operation").first()[0] == "RESTORE"
    assert current_version.select("operationParameters").first()[0]["version"] == str(
        expected_version
    )


def test__apply_uncommitted_migrations__when_schema_migration_insert_fails_on_second_script_file__it_should_rollback_table_to_version_before(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    _test_configuration(spark, rollback_on_failure=True)
    test_schemas.create_test_tables(spark)
    mocker.patch.object(
        sut,
        sut._insert_executed_sql_script.__name__,
        side_effect=["first_call_ok", Exception("mocked error")],
    )
    migrations = ["migration_test_restore", "migration_test_restore_2"]

    table_version = table_helper.get_current_table_version(
        spark, "test_schema", "test_table"
    )
    expected_version = table_version + 1

    # Act
    with pytest.raises(Exception):
        sut.apply_migration_scripts(migrations)

    # Assert
    history = spark.sql("DESCRIBE HISTORY spark_catalog.test_schema.test_table")
    current_version = history.orderBy(F.desc("version")).limit(1)
    assert current_version.select("operation").first()[0] == "RESTORE"
    assert current_version.select("operationParameters").first()[0]["version"] == str(
        expected_version
    )


def test__apply_uncommitted_migrations__when_rollback_on_failure_is_true__version_is_bumped(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    reset_spark_catalog(spark)
    current_version = table_helper.get_table_version(
        spark, "spark_catalog", "test_schema", "test_table"
    )

    mocker.patch.object(
        sut,
        sut._get_table_versions.__name__,
        return_value=[TableVersion("test_schema.test_table", current_version)],
    )

    migrations = ["migration_test_version"]
    expected_version = current_version + 1
    _test_configuration(spark, rollback_on_failure=True)

    # Act
    sut.apply_migration_scripts(migrations)

    # Assert
    actual_version = table_helper.get_table_version(
        spark, "spark_catalog", "test_schema", "test_table"
    )
    assert expected_version == actual_version


def test__apply_uncommitted_migrations__when_schema_migration_insert_fails_and_no_rollback__it_should_not_rollback_table(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    _test_configuration(spark, rollback_on_failure=False)
    test_schemas.create_test_tables(spark)
    mocker.patch.object(
        sut,
        sut._insert_executed_sql_script.__name__,
        side_effect=Exception("mocked error"),
    )

    migrations = ["migration_test_restore"]

    first_version = table_helper.get_current_table_version(
        spark, "test_schema", "test_table"
    )
    expected_version = first_version + 1  # 1 script should have been executed

    # Act
    with pytest.raises(Exception):
        sut.apply_migration_scripts(migrations)

    # Assert
    actual_version = table_helper.get_table_version(
        spark, "spark_catalog", "test_schema", "test_table"
    )
    assert expected_version == actual_version


def test__apply_uncommitted_migrations__when_schema_migration_insert_fails_on_second_script_file_and_no_rollback__it_should_not_rollback_table(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    _test_configuration(spark, rollback_on_failure=False)
    reset_spark_catalog(spark)
    test_schemas.create_test_tables(spark)
    mocker.patch.object(
        sut,
        sut._insert_executed_sql_script.__name__,
        side_effect=["first_call_ok", Exception("mocked error")],
    )
    migrations = ["migration_test_restore", "migration_test_restore_2"]

    first_version = table_helper.get_current_table_version(
        spark, "test_schema", "test_table"
    )
    expected_version = first_version + 2  # 2 scripts should have executed

    # Act
    with pytest.raises(Exception):
        sut.apply_migration_scripts(migrations)

    # Assert
    actual_version = table_helper.get_table_version(
        spark, "spark_catalog", "test_schema", "test_table"
    )
    assert expected_version == actual_version


def test__insert_executed_sql_script__should_insert_row_into_executed_migrations_table(
    spark: SparkSession,
) -> None:
    # Arrange
    reset_spark_catalog(spark)
    migration_name = "test_name"
    configuration = _test_configuration(spark)

    # Act
    sut._insert_executed_sql_script(migration_name)

    # Assert
    actual = spark.table(
        f"{configuration.catalog_name}.{configuration.migration_schema_name}.{configuration.migration_table_name}"
    ).collect()
    assert len(actual) == 1
    assert actual[0].migration_name == migration_name


def test__apply_uncommitted_migrations__when_table_containing_go_in_column_name__it_should_split_queries(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    reset_spark_catalog(spark)
    mocker.patch.object(
        sut,
        sut._get_table_versions.__name__,
        return_value=[TableVersion("test_schema.test_table", 0)],
    )

    migrations = ["migration_test_go"]
    _test_configuration(spark)

    # Act
    sut.apply_migration_scripts(migrations)

    # Assert
    assert spark.catalog.databaseExists("spark_catalog.test_schema")
    assert spark.catalog.tableExists("spark_catalog.test_schema.test_table")


def test__get_table_versions__should_contain_all_tables(spark: SparkSession) -> None:
    # Arrange
    reset_spark_catalog(spark)

    for schema in test_schemas.schema_config:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS spark_catalog.{schema.name}")
        for table in schema.tables:
            schema_df = spark.createDataFrame([], schema=table.schema)
            ddl = schema_df._jdf.schema().toDDL()
            spark.sql(
                f"CREATE TABLE spark_catalog.{schema.name}.{table.name} ({ddl}) USING DELTA LOCATION '{schema.name}/{table.name}'"
            )

    # Act
    actual = sut._get_table_versions()

    # Assert
    for schema in test_schemas.schema_config:
        for table in schema.tables:
            table_version = TableVersion(f"spark_catalog.{schema.name}.{table.name}", 0)
            assert any(
                table_version.table_name == actual_table_version.table_name
                and table_version.version == actual_table_version.version
                for actual_table_version in actual
            )
