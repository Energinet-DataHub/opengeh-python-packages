from importlib.resources import files

from dependency_injector.wiring import Provide, inject
from pyspark.sql import SparkSession

import opengeh_common.migrations.utility.delta_table_helper as delta_table_helper
from opengeh_common.migrations.constants.migrations_constants import ColNames
from opengeh_common.migrations.container import SparkSqlMigrationsContainer
from opengeh_common.migrations.models.configuration import Configuration
from opengeh_common.migrations.schemas.migrations_schema import (
    schema_migration_schema,
)


def get_uncommitted_migration_scripts() -> list[str]:
    all_migrations = get_all_migration_scripts()
    committed_migrations = _get_committed_migration_scripts()

    uncommitted_migrations = [m for m in all_migrations if m not in committed_migrations]

    uncommitted_migrations.sort()
    return uncommitted_migrations


def get_all_migration_scripts() -> list[str]:
    return _get_all_migration_scripts()


@inject
def _get_all_migration_scripts(
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> list[str]:
    migration_files = [p.name for p in files(config.migration_scripts_folder_path).iterdir()]
    migration_files.sort()
    return [file.removesuffix(".sql") for file in migration_files if file.endswith(".sql")]


@inject
def _get_committed_migration_scripts(
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
) -> list[str]:
    table_name = f"{config.table_prefix}{config.migration_table_name}"
    if not delta_table_helper.delta_table_exists(spark, config.catalog_name, config.migration_schema_name, table_name):
        _create_schema_migration_table(config.migration_schema_name, table_name)

    schema_table = spark.table(f"{config.catalog_name}.{config.migration_schema_name}.{table_name}")
    return [row.migration_name for row in schema_table.select(ColNames.migration_name).collect()]


@inject
def _create_schema_migration_table(
    schema_name: str,
    table_name: str,
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> None:
    schema_exists = delta_table_helper.schema_exists(spark, config.catalog_name, schema_name)
    if not schema_exists:
        raise Exception(f"Schema {schema_name} does not exist")

    delta_table_helper.create_table_from_schema(
        spark,
        config.catalog_name,
        schema_name,
        table_name,
        schema_migration_schema,
    )
