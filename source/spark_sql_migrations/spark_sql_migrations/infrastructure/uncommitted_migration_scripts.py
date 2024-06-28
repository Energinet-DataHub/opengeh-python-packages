import spark_sql_migrations.utility.delta_table_helper as delta_table_helper
from pyspark.sql import SparkSession
from importlib.resources import contents
from dependency_injector.wiring import Provide, inject
from spark_sql_migrations.models.configuration import Configuration
from spark_sql_migrations.container import SparkSqlMigrationsContainer
from spark_sql_migrations.constants.migrations_constants import ColNames
from spark_sql_migrations.schemas.migrations_schema import schema_migration_schema


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
    migration_files = list(contents(config.migration_scripts_folder_path))

    migration_files.sort()
    return [file.removesuffix(".sql") for file in migration_files if file.endswith(".sql")]


@inject
def _get_committed_migration_scripts(
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
) -> list[str]:
    table_name = f"{config.table_prefix}{config.migration_table_name}"
    if not delta_table_helper.delta_table_exists(spark, config.catalog_name, config.migration_schema_name, table_name):
        _create_schema_migration_table(config.migration_catalog_name, config.migration_schema_name, table_name)

    schema_table = spark.table(f"{config.catalog_name}.{config.migration_schema_name}.{table_name}")
    return [row.migration_name for row in schema_table.select(ColNames.migration_name).collect()]


@inject
def _create_schema_migration_table(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> None:

    delta_table_helper.create_schema(
        spark,
        catalog_name,
        schema_name,
        "Contains executed SQL migration_scripts",
        config.migration_schema_location
    )

    delta_table_helper.create_table_from_schema(
        spark,
        config.catalog_name,
        schema_name,
        table_name,
        schema_migration_schema,
        location=config.migration_table_location,
    )
