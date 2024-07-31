from spark_sql_migrations.models.schema import Schema
from tests.helpers.schema_migration_costants import SchemaMigrationConstants
from spark_sql_migrations.models.configuration import Configuration
import tests.builders.spark_sql_migrations_configuration_builder as spark_sql_migrations_configuration_builder
import tests.helpers.test_schemas as test_schemas


substitution_variables = {"substitution_variables": "substitution_variables"}


def build(
    current_state_schemas_folder_path: str = "tests.test_scripts.schema_scripts",
    current_state_tables_folder_path: str = "tests.test_scripts.table_scripts",
    current_state_views_folder_path: str = "tests.test_scripts.views_scripts",
    migration_scripts_folder_path: str = "tests.test_scripts.migration_scripts",
    migration_schema_name: str = SchemaMigrationConstants.schema_name,
    migration_table_name: str = SchemaMigrationConstants.table_name,
    table_prefix: str = "",
    catalog_name: str = "spark_catalog",
    schema_config: list[Schema] | None = None,
    substitutions: dict[str, str] | None = None,
) -> Configuration:
    if schema_config is None:
        schema_config = test_schemas.schema_config

    if substitutions is None:
        substitutions = substitution_variables

    spark_config = spark_sql_migrations_configuration_builder.build(
        current_state_schemas_folder_path=current_state_schemas_folder_path,
        current_state_tables_folder_path=current_state_tables_folder_path,
        current_state_views_folder_path=current_state_views_folder_path,
        migration_scripts_folder_path=migration_scripts_folder_path,
        migration_schema_name=migration_schema_name,
        migration_table_name=migration_table_name,
        table_prefix=table_prefix,
        catalog_name=catalog_name,
        schema_config=schema_config,
        substitutions=substitutions,
    )

    return Configuration(spark_sql_migrations_configuration=spark_config)
