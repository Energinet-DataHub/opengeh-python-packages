from dependency_injector import containers, providers
from pyspark.sql import SparkSession
from spark_sql_migrations.models.spark_sql_migrations_configuration import SparkSqlMigrationsConfiguration
import spark_sql_migrations


class SparkSqlMigrationsContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    spark = providers.Singleton(SparkSession.builder.getOrCreate)


def create_and_configure_container(config: SparkSqlMigrationsConfiguration) -> None:
    container = SparkSqlMigrationsContainer()

    _configuration(container, config)

    container.wire(packages=[spark_sql_migrations])


def _configuration(container: SparkSqlMigrationsContainer, config: SparkSqlMigrationsConfiguration) -> None:
    config_dict = vars(config)
    container.config.from_dict(config_dict)
