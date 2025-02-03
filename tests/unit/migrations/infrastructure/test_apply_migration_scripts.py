from pyspark.sql import SparkSession

import opengeh_common.migrations.infrastructure.apply_migration_scripts as sut
import tests.unit.migrations.builders.spark_sql_migrations_configuration_builder as configuration_builder
import tests.unit.migrations.helpers.table_helper as table_helper
from opengeh_common.migrations.container import create_and_configure_container
from opengeh_common.migrations.models.spark_sql_migrations_configuration import (
    SparkSqlMigrationsConfiguration,
)
from opengeh_common.migrations.schemas.migrations_schema import (
    schema_migration_schema,
)
from tests.unit.migrations.constants import TEST_SCRIPTS_DIR
from tests.unit.migrations.helpers.spark_helper import reset_spark_catalog

shared_storage_account = "shared_storage_account"
storage_account = "storage_account"


def _test_configuration(spark: SparkSession) -> SparkSqlMigrationsConfiguration:
    configuration = configuration_builder.build(
        migration_scripts_folder_path=TEST_SCRIPTS_DIR,
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
    spark: SparkSession,
) -> None:
    # Arrange
    reset_spark_catalog(spark)

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
    spark: SparkSession,
) -> None:
    # Arrange
    reset_spark_catalog(spark)

    migrations = ["migration_test_go"]
    _test_configuration(spark)

    # Act
    sut.apply_migration_scripts(migrations)

    # Assert
    assert spark.catalog.databaseExists("spark_catalog.test_schema")
    assert spark.catalog.tableExists("spark_catalog.test_schema.test_table")
