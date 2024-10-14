import spark_sql_migrations
from pyspark.sql import SparkSession
from dependency_injector import containers, providers
from spark_sql_migrations.models.spark_sql_migrations_configuration import SparkSqlMigrationsConfiguration


class SparkSqlMigrationsContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    spark = providers.Singleton(SparkSession.builder.getOrCreate)


def create_and_configure_container(config: SparkSqlMigrationsConfiguration) -> None:
    container = SparkSqlMigrationsContainer()

    container.config.from_value(config)
    # weird change
    container.wire(packages=[spark_sql_migrations])
