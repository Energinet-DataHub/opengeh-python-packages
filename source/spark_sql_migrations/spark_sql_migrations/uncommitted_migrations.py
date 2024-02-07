import spark_sql_migrations.utility.delta_table_helper as delta_table_helper
from pyspark.sql import SparkSession
from importlib.resources import contents
from dependency_injector.wiring import Provide, inject
from spark_sql_migrations.container import SparkSqlMigrationsContainer
from spark_sql_migrations.constants.migrations_constants import ColNames
from spark_sql_migrations.schemas.migrations_schema import schema_migration_schema


def get_uncommitted_migrations() -> list[str]:
    all_migrations = get_all_migrations()
    committed_migrations = _get_committed_migrations()

    uncommitted_migrations = [m for m in all_migrations if m not in committed_migrations]

    uncommitted_migrations.sort()
    return uncommitted_migrations


def get_all_migrations() -> list[str]:
    migration_files = list(contents("package.schema_migration.migration_scripts"))

    migration_files.sort()
    return [file.removesuffix(".sql") for file in migration_files if file.endswith(".sql")]


@inject
def _get_committed_migrations(
    migration_schema_name: str = Provide[SparkSqlMigrationsContainer.config.migration_schema_name],
    migration_table_name: str = Provide[SparkSqlMigrationsContainer.config.migration_table_name],
    table_prefix: str = Provide[SparkSqlMigrationsContainer.config.table_prefix],
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
) -> list[str]:
    table_name = f"{table_prefix}{migration_table_name}"
    if not delta_table_helper.delta_table_exists(spark, migration_schema_name, table_name):
        _create_schema_migration_table(spark, migration_schema_name, table_name)

    schema_table = spark.table(f"{migration_schema_name}.{table_name}")
    return [row.migration_name for row in schema_table.select(ColNames.migration_name).collect()]


@inject
def _create_schema_migration_table(
    spark: SparkSession,
    schema_name: str,
    table_name: str,
    schema_location: str = Provide[SparkSqlMigrationsContainer.config.migration_schema_location],
    table_location: str = Provide[SparkSqlMigrationsContainer.config.migration_table_location],
) -> None:
    delta_table_helper.create_schema(
        spark, schema_name, "Contains executed SQL migration_scripts", schema_location
    )

    delta_table_helper.create_table_from_schema(
        spark,
        schema_name,
        table_name,
        schema_migration_schema,
        location=table_location,
    )
