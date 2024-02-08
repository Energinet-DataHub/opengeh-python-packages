import spark_sql_migrations.migrations.sql_file_executor as sql_file_executor
from importlib.resources import contents
from dependency_injector.wiring import Provide, inject
from spark_sql_migrations.container import SparkSqlMigrationsContainer
from spark_sql_migrations.models.configuration import Configuration


def create_all_tables() -> None:
    _create_all_tables()


@inject
def _create_all_tables(
    config: Configuration = Provide[SparkSqlMigrationsContainer.configuration]
) -> None:
    """Executes schema and table scripts to create all tables"""
    print("Creating all tables")
    schema_scripts = _get_schema_scripts()
    table_scripts = _get_table_scripts()

    try:
        for script in schema_scripts:
            sql_file_executor.execute(
                script, config.current_state_schemas_folder_path
            )

        for script in table_scripts:
            sql_file_executor.execute(
                script, config.current_state_tables_folder_path
            )
    except Exception as exception:
        print(f"Schema migration failed with exception: {exception}")
        raise exception

    print("Successfully created all tables")


@inject
def _get_schema_scripts(
        config: Configuration = Provide[SparkSqlMigrationsContainer.configuration]
) -> list[str]:
    migration_files = list(contents(config.current_state_schemas_folder_path))
    return [file.removesuffix(".sql") for file in migration_files if file.endswith(".sql")]


@inject
def _get_table_scripts(
    config: Configuration = Provide[SparkSqlMigrationsContainer.configuration]
) -> list[str]:
    migration_files = list(contents(config.current_state_tables_folder_path))
    return [file.removesuffix(".sql") for file in migration_files if file.endswith(".sql")]
