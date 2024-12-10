import spark_sql_migrations.infrastructure.uncommitted_migration_scripts as uncommitted_migrations
import spark_sql_migrations.infrastructure.apply_migration_scripts as apply_migrations
import spark_sql_migrations.current_state.create_current_state as create_current_state
from pyspark.sql import SparkSession
from dependency_injector.wiring import Provide, inject
from spark_sql_migrations.container import SparkSqlMigrationsContainer
from spark_sql_migrations.models.configuration import Configuration


def migrate() -> None:
    tables = _get_tables()

    # This is temporary solution to migrate without current state.
    if tables is None:
        _migrate_without_current_state()
    else:
        _migrate(len(_get_tables()))


def _migrate_without_current_state() -> None:
    """
    This is temporary solution to migrate without current state.
    """
    migrations: list[str] = uncommitted_migrations.get_uncommitted_migration_scripts()
    if len(migrations) > 0:
        (apply_migrations.apply_migration_scripts(migrations))


def _migrate(existing_tables_count: int) -> None:
    all_migrations_count: int = len(uncommitted_migrations.get_all_migration_scripts())
    migrations: list[str] = uncommitted_migrations.get_uncommitted_migration_scripts()

    print(f"Existing table count: {existing_tables_count}")
    print(f"Uncommitted migrations count: {len(migrations)}")
    print(f"All migrations count: {all_migrations_count}")

    if existing_tables_count == 0:
        if len(migrations) == 0:
            create_current_state.create_all_tables()
        elif len(migrations) == all_migrations_count:
            (apply_migrations.apply_migration_scripts(migrations))
        else:
            raise Exception(
                "Uncommitted migrations are not in sync with all migrations"
            )
    else:
        if len(migrations) != 0:
            apply_migrations.apply_migration_scripts(migrations)
        missing_tables = _get_missing_tables()
        if len(missing_tables) != 0:
            create_current_state.create_all_tables()


@inject
def _get_tables(
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
) -> list[str] | None:
    tables = []

    # This is temporary solution to migrate without current state.
    if config.schema_config is None:
        return None

    for schema in config.schema_config:
        if spark.catalog.databaseExists(f"{config.catalog_name}.{schema.name}") is True:
            for table in schema.tables:
                table_name = f"{config.catalog_name}.{schema.name}.{table.name}"
                if spark.catalog.tableExists(table_name):
                    tables.append(table_name)

    return tables


@inject
def _get_missing_tables(
    config: Configuration = Provide[SparkSqlMigrationsContainer.config],
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
) -> list[str]:
    missing_tables = []
    for schema in config.schema_config:
        for table in schema.tables:
            table_name = f"{config.catalog_name}.{schema.name}.{table.name}"
            if spark.catalog.tableExists(table_name) is False:
                missing_tables.append(table_name)

    return missing_tables
