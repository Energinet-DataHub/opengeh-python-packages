from pyspark.sql import SparkSession

import spark_sql_migrations.infrastructure.sql_file_executor as sql_file_executor
from importlib.resources import contents
from dependency_injector.wiring import Provide, inject
from spark_sql_migrations.container import SparkSqlMigrationsContainer
from spark_sql_migrations.models.configuration import Configuration
from spark_sql_migrations.utility import delta_table_helper
from spark_sql_migrations.schemas.migrations_schema import schema_migration_schema


def create_all_tables() -> None:
    _create_all_tables()


@inject
def _create_all_tables(
        config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> None:
    """Executes schema, table and view scripts to create all tables"""

    print("Creating all tables")

    schema_scripts = _get_schema_scripts()
    table_scripts = _get_table_scripts()
    view_scripts = _get_view_scripts()

    print(
        f"Found {len(schema_scripts)} schema scripts in {config.current_state_schemas_folder_path}"
    )
    print(
        f"Found {len(table_scripts)} table scripts in {config.current_state_tables_folder_path}"
    )
    print(
        f"Found {len(view_scripts)} view scripts in {config.current_state_views_folder_path}"
    )

    _create_schema_migration_table()
    for script in schema_scripts:
        sql_file_executor.execute(script, config.current_state_schemas_folder_path)

    for script in table_scripts:
        sql_file_executor.execute(script, config.current_state_tables_folder_path)

    for script in view_scripts:
        sql_file_executor.execute(script, config.current_state_views_folder_path)

    print("Successfully created all tables")


@inject
def _create_schema_migration_table(
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> None:
    schema_exists = delta_table_helper.schema_exists(
        spark, config.catalog_name, config.migration_schema_name
    )
    if not schema_exists:
        raise Exception(f"Schema {config.migration_schema_name} does not exist")

    table_name = f"{config.table_prefix}{config.migration_table_name}"
    if not delta_table_helper.delta_table_exists(
        spark, config.catalog_name, config.migration_schema_name, table_name
    ):
        delta_table_helper.create_table_from_schema(
            spark,
            config.catalog_name,
            config.migration_schema_name,
            table_name,
            schema_migration_schema
        )


@inject
def _get_schema_scripts(
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> list[str]:
    if len(config.current_state_schemas_folder_path) == 0:
        return []
    migration_files = contents(config.current_state_schemas_folder_path)
    return [
        file.removesuffix(".sql") for file in migration_files if file.endswith(".sql")
    ]


@inject
def _get_table_scripts(
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> list[str]:
    if len(config.current_state_tables_folder_path) == 0:
        return []
    migration_files = contents(config.current_state_tables_folder_path)
    return [
        file.removesuffix(".sql") for file in migration_files if file.endswith(".sql")
    ]


@inject
def _get_view_scripts(
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
) -> list[str]:
    if len(config.current_state_views_folder_path) == 0:
        return []
    view_script_files = contents(config.current_state_views_folder_path)
    script_files = [
        view_script_file.removesuffix(".sql")
        for view_script_file in view_script_files
        if view_script_file.endswith(".sql")
    ]
    script_files.sort()
    return script_files
