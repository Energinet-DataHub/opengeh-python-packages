from typing import List
from spark_sql_migrations.models.schema import Schema


class SparkSqlMigrationsConfiguration:
    def __init__(
            self,
            migration_schema_name: str,
            migration_schema_location: str,
            migration_table_name: str,
            migration_table_location: str,
            migration_scripts_folder_path: str,
            db_folder: str,
            table_prefix: str,
            current_state_schemas_folder_path: str,
            current_state_tables_folder_path: str,
            schema_config: any,
            substitution_variables: dict[str, str],
    ) -> None:
        self.migration_scripts_folder_path = migration_scripts_folder_path
        self.migration_schema_name = migration_schema_name
        self.migration_schema_location = migration_schema_location
        self.migration_table_name = migration_table_name
        self.migration_table_location = migration_table_location
        self.db_folder = db_folder
        self.table_prefix = table_prefix
        self.current_state_schemas_folder_path = current_state_schemas_folder_path
        self.current_state_tables_folder_path = current_state_tables_folder_path
        self.schema_config = schema_config
        self.substitution_variables = substitution_variables

