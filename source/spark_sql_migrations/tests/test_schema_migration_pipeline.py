from unittest.mock import Mock
from pyspark.sql import SparkSession
from tests.helpers import spark_helper
from tests.helpers.test_schemas import schema_config
from tests.helpers.schema_migration_costants import SchemaMigrationConstants
import pytest
import tests.helpers.mock_helper as mock_helper
import tests.helpers.table_helper as table_helper
import spark_sql_migrations.schema_migration_pipeline as sut


def test_migrate_with_schema_migration_scripts_compare_schemas(
    spark: SparkSession,
) -> None:
    # Arrange
    spark_helper.reset_spark_catalog(spark)
    table_helper.create_schema(
        spark,
        SchemaMigrationConstants.catalog_name,
        SchemaMigrationConstants.schema_name,
    )

    # Act
    sut.migrate()

    # Assert
    assert True


def test_migrate_with_schema_migration_scripts_compare_result_with_schema_config(
    spark: SparkSession,
) -> None:
    """If this test fails, it indicates that a SQL script is creating something that the Schema Config does not know
    about"""
    # Arrange
    spark_helper.reset_spark_catalog(spark)
    table_helper.create_schema(
        spark,
        SchemaMigrationConstants.catalog_name,
        SchemaMigrationConstants.schema_name,
    )

    # Act
    sut.migrate()

    # Assert
    catalog_name = spark.catalog.currentCatalog()
    schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
    for schema in schemas.collect():
        schema_name = schema["namespace"]
        if schema_name == "default" or schema_name == "schema_migration":
            continue

        actual_schema = next((x for x in schema_config if x.name == schema_name), None)
        assert (
            actual_schema is not None
        ), f"Schema {schema_name} is not in the schema config"
        tables = spark.catalog.listTables(f"{catalog_name}.{schema_name}")
        for table in tables:
            if table.tableType == "VIEW":
                continue

            actual_table = spark.table(f"{catalog_name}.{schema_name}.{table.name}")
            table_config = next(
                (x for x in actual_schema.tables if x.name == table.name), None
            )
            assert (
                table_config is not None
            ), f"Table {table.name} is not in the schema config"
            assert actual_table.schema == table_config.schema


def test_migrate_with_0_tables_and_0_migrations_should_call_create_tables(
    mocker: Mock,
    spark: SparkSession,
) -> None:
    # Arrange
    mocker.patch.object(sut, sut._get_tables.__name__, return_value=[])
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_uncommitted_migration_scripts.__name__,
        return_value=[],
    )
    mock_create_all_tables = mocker.patch.object(
        sut.create_current_state,
        sut.create_current_state.create_all_tables.__name__,
        side_effect=mock_helper.do_nothing,
    )

    spark_helper.reset_spark_catalog(spark)

    # Act
    sut.migrate()

    # Assert
    mock_create_all_tables.assert_called_once()


def test_migrate_with_0_tables_and_all_migrations_should_call_apply_migrations(
    mocker: Mock,
    spark: SparkSession,
) -> None:
    # Arrange
    mocker.patch.object(sut, sut._get_tables.__name__, return_value=[])
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_uncommitted_migration_scripts.__name__,
        return_value=["migration1", "migration2"],
    )
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_all_migration_scripts.__name__,
        return_value=["migration1", "migration2"],
    )

    mocked_apply_uncommitted_migrations = mocker.patch.object(
        sut.apply_migrations,
        sut.apply_migrations.apply_migration_scripts.__name__,
        side_effect=mock_helper.do_nothing,
    )

    spark_helper.reset_spark_catalog(spark)

    # Act
    sut.migrate()

    # Assert
    mocked_apply_uncommitted_migrations.assert_called_once()


def test_migrate_with_0_tables_and_1_migration_should_throw_exception(
    mocker: Mock,
    spark: SparkSession,
) -> None:
    # Arrange
    mocker.patch.object(sut, sut._get_tables.__name__, return_value=[])
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_uncommitted_migration_scripts.__name__,
        return_value=["migration1"],
    )
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_all_migration_scripts.__name__,
        return_value=["migration1", "migration2", "migration3"],
    )

    spark_helper.reset_spark_catalog(spark)

    # Act / Assert
    with pytest.raises(Exception):
        sut.migrate()


def test_migrate_with_0_tables_and_almost_all_migrations_should_throw_exception(
    mocker: Mock,
    spark: SparkSession,
) -> None:
    # Arrange
    mocker.patch.object(
        sut, sut._get_tables.__name__, return_value=mock_helper.do_nothing
    )

    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_uncommitted_migration_scripts.__name__,
        return_value=["migration1", "migration2", "migration3"],
    )

    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_all_migration_scripts.__name__,
        return_value=["migration1", "migration2", "migration3", "migration4"],
    )

    spark_helper.reset_spark_catalog(spark)

    # Act / Assert
    with pytest.raises(Exception):
        sut.migrate()


def test_migrate_with_all_tables_when_one_uncommitted_migration_should_run_migrations(
    mocker: Mock,
    spark: SparkSession,
) -> None:
    # Arrange
    mocker.patch.object(sut, sut._get_tables.__name__, return_value=["table1"])
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_uncommitted_migration_scripts.__name__,
        return_value=["migration1"],
    )
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_all_migration_scripts.__name__,
        return_value=["migration1", "migration2"],
    )
    mocker.patch.object(
        sut.create_current_state,
        sut.create_current_state.create_all_tables.__name__,
        side_effect=mock_helper.do_nothing,
    )
    mocker.patch.object(sut, sut._get_missing_tables.__name__, return_value=[])

    mocked_apply_uncommitted_migrations = mocker.patch.object(
        sut.apply_migrations,
        sut.apply_migrations.apply_migration_scripts.__name__,
        side_effect=mock_helper.do_nothing,
    )

    spark_helper.reset_spark_catalog(spark)

    # Act
    sut.migrate()

    # Assert
    mocked_apply_uncommitted_migrations.assert_called_once()


def test_migrate_when_one_table_missing_and_no_migrations_should_call_create_tables(
    mocker: Mock,
    spark: SparkSession,
) -> None:
    # Arrange
    mocker.patch.object(sut, sut._get_tables.__name__, return_value=["table1"])
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_uncommitted_migration_scripts.__name__,
        return_value=[],
    )
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_all_migration_scripts.__name__,
        return_value=["migration1", "migration2"],
    )

    mocked_create_all_tables = mocker.patch.object(
        sut.create_current_state,
        sut.create_current_state.create_all_tables.__name__,
        side_effect=mock_helper.do_nothing,
    )

    spark_helper.reset_spark_catalog(spark)

    # Act
    sut.migrate()

    # Assert
    mocked_create_all_tables.assert_called_once()


def test_migrate_with_none_tables_and_uncommitted_migrations_throws_exception(
    mocker: Mock,
    spark: SparkSession,
) -> None:
    # Arrange
    mocker.patch.object(sut, sut._get_tables.__name__, return_value=None)
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_all_migration_scripts.__name__,
        return_value=[],
    )
    # Act / Assert
    with pytest.raises(Exception):
        sut.migrate()


def test_migrate_with_none_tables_and_uncommitted_migrations(
    mocker: Mock,
    spark: SparkSession,
) -> None:
    # Arrange
    mocker.patch.object(sut, sut._get_tables.__name__, return_value=None)
    mocked_apply_migration_scripts = mocker.patch.object(
        sut.apply_migrations,
        sut.apply_migrations.apply_migration_scripts.__name__,
        return_value=None
    )
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_all_migration_scripts.__name__,
        return_value=["migration1", "migration2"],
    )

    # Act
    sut.migrate()

    # Assert
    mocked_apply_migration_scripts.assert_called_once()


def test_migrate_with_none_tables_and_zero_uncommitted_migrations_throws_exception(
    mocker: Mock,
    spark: SparkSession,
) -> None:
    # Arrange
    mocker.patch.object(sut, sut._get_tables.__name__, return_value=None)
    mocker.patch.object(
        sut.uncommitted_migrations,
        sut.uncommitted_migrations.get_all_migration_scripts.__name__,
        return_value=[],
    )
    # Act / Assert
    with pytest.raises(Exception):
        sut.migrate()
