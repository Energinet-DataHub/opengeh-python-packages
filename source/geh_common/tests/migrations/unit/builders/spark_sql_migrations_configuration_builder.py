from geh_common.migrations.models.spark_sql_migrations_configuration import (
    SparkSqlMigrationsConfiguration,
)
from tests.migrations.unit.constants import MIGRATION_TEST_SCRIPTS_DIR
from tests.migrations.unit.helpers.schema_migration_costants import (
    SchemaMigrationConstants,
)

substitution_variables = {"substitution_variables": "substitution_variables"}


def build(
    catalog_name: str = SchemaMigrationConstants.catalog_name,
    migration_scripts_folder_path: str = f"{MIGRATION_TEST_SCRIPTS_DIR}.migration_scripts",
    migration_schema_name: str = SchemaMigrationConstants.schema_name,
    migration_table_name: str = SchemaMigrationConstants.table_name,
    table_prefix: str = "",
    substitutions: dict[str, str] | None = None,
) -> SparkSqlMigrationsConfiguration:
    if substitutions is None:
        substitutions = substitution_variables

    return SparkSqlMigrationsConfiguration(
        catalog_name=catalog_name,
        migration_scripts_folder_path=migration_scripts_folder_path,
        migration_schema_name=migration_schema_name,
        migration_table_name=migration_table_name,
        table_prefix=table_prefix,
        substitution_variables=substitutions,
    )
