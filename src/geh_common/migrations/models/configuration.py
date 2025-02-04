from geh_common.migrations.models.spark_sql_migrations_configuration import (
    SparkSqlMigrationsConfiguration,
)


class Configuration:
    def __init__(
        self, spark_sql_migrations_configuration: SparkSqlMigrationsConfiguration
    ) -> None:
        self.migration_scripts_folder_path = (
            spark_sql_migrations_configuration.migration_scripts_folder_path
        )
        self.catalog_name = spark_sql_migrations_configuration.catalog_name
        self.migration_schema_name = (
            spark_sql_migrations_configuration.migration_schema_name
        )
        self.migration_table_name = (
            spark_sql_migrations_configuration.migration_table_name
        )
        self.table_prefix = spark_sql_migrations_configuration.table_prefix
        self.substitution_variables = (
            spark_sql_migrations_configuration.substitution_variables
        )
