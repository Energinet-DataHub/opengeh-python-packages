from spark_sql_migrations.models.schema import Schema
from spark_sql_migrations.models.spark_sql_migrations_configuration import (
    SparkSqlMigrationsConfiguration,
)
from tests.helpers.schema_migration_costants import SchemaMigrationConstants
import tests.helpers.test_schemas as test_schemas


substitution_variables = {"substitution_variables": "substitution_variables"}


def build(
    catalog_name: str = SchemaMigrationConstants.catalog_name,
    current_state_schemas_folder_path: str = "tests.test_scripts.schema_scripts",
    current_state_tables_folder_path: str = "tests.test_scripts.table_scripts",
    current_state_views_folder_path: str = "tests.test_scripts.view_scripts",
    migration_scripts_folder_path: str = "tests.test_scripts.migration_scripts",
    migration_schema_name: str = SchemaMigrationConstants.schema_name,
    migration_table_name: str = SchemaMigrationConstants.table_name,
    table_prefix: str = "",
    rollback_on_failure: bool = False,
    schema_config: list[Schema] | None = None,
    substitutions: dict[str, str] | None = None,
) -> SparkSqlMigrationsConfiguration:
    if schema_config is None:
        schema_config = test_schemas.schema_config

    if substitutions is None:
        substitutions = substitution_variables

    return SparkSqlMigrationsConfiguration(
        catalog_name=catalog_name,
        current_state_schemas_folder_path=current_state_schemas_folder_path,
        current_state_tables_folder_path=current_state_tables_folder_path,
        current_state_views_folder_path=current_state_views_folder_path,
        migration_scripts_folder_path=migration_scripts_folder_path,
        migration_schema_name=migration_schema_name,
        migration_table_name=migration_table_name,
        table_prefix=table_prefix,
        schema_config=schema_config,
        substitution_variables=substitutions,
        rollback_on_failure=rollback_on_failure,
    )
