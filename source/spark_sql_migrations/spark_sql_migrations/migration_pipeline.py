import spark_sql_migrations.infrastructure.uncommitted_migration_scripts as uncommitted_migrations
import spark_sql_migrations.infrastructure.apply_migration_scripts as apply_migrations
from spark_sql_migrations.container import create_and_configure_container
from spark_sql_migrations.models.spark_sql_migrations_configuration import SparkSqlMigrationsConfiguration


def migrate(configuration: SparkSqlMigrationsConfiguration) -> None:
    _configure_spark_sql_migrations(configuration)
    _migrate()


def _migrate() -> None:
    migrations: list[str] = uncommitted_migrations.get_uncommitted_migration_scripts()
    if len(migrations) > 0:
        (apply_migrations.apply_migration_scripts(migrations))


def _configure_spark_sql_migrations(configuration: SparkSqlMigrationsConfiguration) -> None:
    create_and_configure_container(configuration)
