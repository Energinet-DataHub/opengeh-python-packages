from spark_sql_migrations.models.spark_sql_migrations_configuration import (
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
        self.current_state_schemas_folder_path = (
            spark_sql_migrations_configuration.current_state_schemas_folder_path
        )
        self.current_state_tables_folder_path = (
            spark_sql_migrations_configuration.current_state_tables_folder_path
        )
        self.current_state_views_folder_path = (
            spark_sql_migrations_configuration.current_state_views_folder_path
        )
        self.schema_config = spark_sql_migrations_configuration.schema_config
        self.substitution_variables = (
            spark_sql_migrations_configuration.substitution_variables
        )
        self.rollback_on_failure = (
            spark_sql_migrations_configuration.rollback_on_failure
        )
