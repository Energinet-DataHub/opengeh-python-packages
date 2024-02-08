import pyspark.sql.functions as F
import spark_sql_migrations.migrations.sql_file_executor as sql_file_executor
from pyspark.sql import SparkSession
from spark_sql_migrations.migrations.table_version import TableVersion
from dependency_injector.wiring import Provide, inject
from spark_sql_migrations.container import SparkSqlMigrationsContainer
from typing import List
from spark_sql_migrations.models.schema import Schema


def apply_uncommitted_migrations(uncommitted_migrations: list[str]) -> None:
    for migration in uncommitted_migrations:
        table_versions = _get_table_versions()
        try:
            sql_file_executor.execute(
                migration
            )
            _insert_executed_sql_script(migration)

        except Exception as exception:
            print(f"Schema migration failed with exception: {exception}")

            for table_version in table_versions:
                _restore_table(table_version)

            raise exception


@inject
def _get_table_versions(
        config: any = Provide[SparkSqlMigrationsContainer.configuration],
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
def _restore_table(
    table_version: TableVersion, spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark]
) -> None:
    spark.sql(f"RESTORE TABLE {table_version.table_name} TO VERSION AS OF {table_version.version}")


@inject
def _insert_executed_sql_script(
    migration_name: str,
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
    db_folder: str = Provide[SparkSqlMigrationsContainer.config.db_folder],
    table_prefix: str = Provide[SparkSqlMigrationsContainer.config.table_prefix],
    migration_schema_name: str = Provide[SparkSqlMigrationsContainer.config.schema_migration_schema],
    migration_table_name: str = Provide[SparkSqlMigrationsContainer.config.schema_migration_table_name],
) -> None:
    schema_name = (
        db_folder
        if db_folder
        else migration_schema_name
    )
    table_name = f"{table_prefix}{migration_table_name}"
    sql_query = f"""
        INSERT INTO {schema_name}.{table_name}
        VALUES ('{migration_name}', current_timestamp())
    """
    spark.sql(sql_query)
