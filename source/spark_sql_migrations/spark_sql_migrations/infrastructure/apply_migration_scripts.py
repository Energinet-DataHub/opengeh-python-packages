import pyspark.sql.functions as F
import spark_sql_migrations.utility.delta_table_helper as delta_table_helper
import spark_sql_migrations.infrastructure.sql_file_executor as sql_file_executor
from typing import List
from pyspark.sql import SparkSession
from dependency_injector.wiring import Provide, inject
from spark_sql_migrations.models.table_version import TableVersion
from spark_sql_migrations.models.configuration import Configuration
from spark_sql_migrations.container import SparkSqlMigrationsContainer


def apply_migration_scripts(
        uncommitted_migrations: List[str]
) -> None:
    _apply_migration_scripts(uncommitted_migrations)


@inject
def _apply_migration_scripts(
        uncommitted_migrations: list[str],
        config: Configuration = Provide[SparkSqlMigrationsContainer.config]
) -> None:
    for migration in uncommitted_migrations:
        table_versions = _get_table_versions()
        try:
            sql_file_executor.execute(
                migration, config.migration_scripts_folder_path
            )
            _insert_executed_sql_script(migration)

        except Exception as exception:
            print(f"Schema migration failed with exception: {exception}")

            for table_version in table_versions:
                delta_table_helper.restore_table(table_version)

            raise exception


@inject
def _get_table_versions(
        config: Configuration = Provide[SparkSqlMigrationsContainer.config],
        spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark]) -> list[TableVersion]:
    tables = []
    for schema in config.schema_config:
        if spark.catalog.databaseExists(schema.name) is True:
            for table in schema.tables:
                if spark.catalog.tableExists(table.name, schema.name) is False:
                    continue

                version = spark.sql(f"DESCRIBE HISTORY {schema.name}.{table.name}")
                version_no = version.select(F.max(F.col("version"))).collect()[0][0]
                table_version = TableVersion(f"{schema.name}.{table.name}", version_no)
                tables.append(table_version)

    return tables


@inject
def _insert_executed_sql_script(
    migration_name: str,
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
    config: Configuration = Provide[SparkSqlMigrationsContainer.config]
) -> None:
    schema_name = (
        config.db_folder
        if config.db_folder
        else config.migration_schema_name
    )
    table_name = f"{config.table_prefix}{config.migration_table_name}"
    sql_query = f"""
        INSERT INTO {schema_name}.{table_name}
        VALUES ('{migration_name}', current_timestamp())
    """
    spark.sql(sql_query)

    spark.sql("DESCRIBE HISTORY schema_migration.executed_migrations")
