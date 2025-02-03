from typing import List

from dependency_injector.wiring import Provide, inject
from pyspark.sql import SparkSession

import opengeh_utilities.migrations.infrastructure.sql_file_executor as sql_file_executor
from opengeh_utilities.migrations.container import SparkSqlMigrationsContainer
from opengeh_utilities.migrations.models.configuration import Configuration


def apply_migration_scripts(uncommitted_migrations: List[str]) -> None:
    _apply_migration_scripts(uncommitted_migrations)


@inject
def _apply_migration_scripts(
    uncommitted_migrations: list[str],
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> None:
    for migration in uncommitted_migrations:
        try:
            sql_file_executor.execute(migration, config.migration_scripts_folder_path)
            _insert_executed_sql_script(migration)
        except Exception as exception:
            print(f"Schema migration failed with exception: {exception}")  # noqa
            raise exception


@inject
def _insert_executed_sql_script(
    migration_name: str,
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> None:
    table_name = f"{config.table_prefix}{config.migration_table_name}"
    sql_query = f"""
        INSERT INTO {config.catalog_name}.{config.migration_schema_name}.{table_name}
        VALUES ('{migration_name}', current_timestamp())
    """
    spark.sql(sql_query)
