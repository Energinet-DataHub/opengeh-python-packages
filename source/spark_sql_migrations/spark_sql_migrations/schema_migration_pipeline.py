import spark_sql_migrations.infrastructure.uncommitted_migration_scripts as uncommitted_migrations
import spark_sql_migrations.infrastructure.apply_migration_scripts as apply_migrations
import spark_sql_migrations.current_state.create_current_state as create_current_state
from pyspark.sql import SparkSession
from dependency_injector.wiring import Provide, inject
from spark_sql_migrations.container import SparkSqlMigrationsContainer
from spark_sql_migrations.models.configuration import Configuration


def migrate() -> None:
    existing_tables_count = len(_get_tables())
    migrations = uncommitted_migrations.get_uncommitted_migration_scripts()
    all_migrations_count = len(uncommitted_migrations.get_all_migration_scripts())

    print(f"Existing table count: {existing_tables_count}")
    print(f"Uncommitted migrations count: {len(migrations)}")
    print(f"All migrations count: {all_migrations_count}")

    if existing_tables_count == 0:
        if len(migrations) == 0:
            create_current_state.create_all_tables()
        elif len(migrations) == all_migrations_count:
            (apply_migrations.apply_migration_scripts(migrations))
        else:
            raise Exception("Uncommitted migrations are not in sync with all migrations")
    else:
        if len(migrations) != 0:
            apply_migrations.apply_migration_scripts(migrations)
        missing_tables = _get_missing_tables()
        if len(missing_tables) != 0:
            create_current_state.create_all_tables()


@inject
def _get_tables(
        config: Configuration = Provide[SparkSqlMigrationsContainer.config],
        spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark]
) -> list[str]:
    tables = []

    for schema in config.schema_config:
        if spark.catalog.databaseExists(schema.name) is True:
            for table in schema.tables:
                table_name = f"{schema.name}.{table.name}"
                if spark.catalog.tableExists(table_name):
                    tables.append(table_name)

    return tables


@inject
def _get_missing_tables(
        config: Configuration = Provide[SparkSqlMigrationsContainer.config],
        spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark]
) -> list[str]:
    missing_tables = []

    for schema in config.schema_config:
        for table in schema.tables:
            table_name = f"{schema.name}.{table.name}"
            if spark.catalog.tableExists(table_name) is False:
                missing_tables.append(table_name)

    return missing_tables
