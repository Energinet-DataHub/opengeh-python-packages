from spark_sql_migrations.models.table import Table
from spark_sql_migrations.models.schema import Schema
from spark_sql_migrations.models.spark_sql_migrations_configuration import SparkSqlMigrationsConfiguration


substitution_variables = {"substitution_variables": "substitution_variables"}
schema_configuration = [Schema(name="test_schema", tables=[Table(name="test_table", schema="test_schema")])]


def build(
        current_state_schemas_folder_path: str = "tests.test_scripts.schema_scripts",
        current_state_tables_folder_path: str = "tests.test_scripts.table_scripts",
        migration_scripts_folder_path: str = "tests.test_scripts.migration_scripts",
        migration_schema_name: str = "migration_schema_name",
        migration_table_name: str = "migration_table_name",
        table_prefix: str = "",
        migration_schema_location: str = "migration_schema_location",
        migration_table_location: str = "migration_table_location",
        db_folder: str = "",
        schema_config: list[Schema] | None = None,
        substitutions: dict[str, str] | None = None
) -> SparkSqlMigrationsConfiguration:
    if schema_config is None:
        schema_config = schema_configuration

    if substitutions is None:
        substitutions = substitution_variables

    return SparkSqlMigrationsConfiguration(
        current_state_schemas_folder_path=current_state_schemas_folder_path,
        current_state_tables_folder_path=current_state_tables_folder_path,
        migration_scripts_folder_path=migration_scripts_folder_path,
        migration_schema_name=migration_schema_name,
        migration_table_name=migration_table_name,
        table_prefix=table_prefix,
        migration_schema_location=migration_schema_location,
        migration_table_location=migration_table_location,
        db_folder=db_folder,
        schema_config=schema_config,
        substitution_variables=substitutions,
    )
