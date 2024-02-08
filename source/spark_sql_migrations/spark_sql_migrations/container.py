from dependency_injector import containers, providers
from pyspark.sql import SparkSession
from spark_sql_migrations.models.spark_sql_migrations_configuration import SparkSqlMigrationsConfiguration
from spark_sql_migrations.models.configuration import Configuration
import spark_sql_migrations

from spark_sql_migrations.models.schema import Schema
from typing import List


class TestSchema:
    schema_config: List[Schema]


class SparkSqlMigrationsContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    spark = providers.Singleton(SparkSession.builder.getOrCreate)

    configuration = providers.Factory()


def create_and_configure_container(config: SparkSqlMigrationsConfiguration) -> None:
    container = SparkSqlMigrationsContainer()

    container.configuration.override(
        providers.Factory(Configuration, spark_sql_migrations_configuration=config)
    )

    container.wire(packages=[spark_sql_migrations])
