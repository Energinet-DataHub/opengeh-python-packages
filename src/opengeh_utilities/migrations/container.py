from dependency_injector import containers, providers
from pyspark.sql import SparkSession

import opengeh_utilities.migrations as spark_sql_migrations
from opengeh_utilities.migrations.models.spark_sql_migrations_configuration import (
    SparkSqlMigrationsConfiguration,
)


class SparkSqlMigrationsContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    spark = providers.Singleton(SparkSession.builder.getOrCreate)  # type: ignore


def create_and_configure_container(config: SparkSqlMigrationsConfiguration) -> None:
    container = SparkSqlMigrationsContainer()

    container.config.from_value(config)

    container.wire(packages=[spark_sql_migrations])
