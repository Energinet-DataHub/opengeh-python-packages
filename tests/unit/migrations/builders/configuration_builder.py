import tests.unit.migrations.builders.spark_sql_migrations_configuration_builder as spark_sql_migrations_configuration_builder
from geh_common.migrations.models.configuration import Configuration
from tests.unit.migrations.constants import TEST_SCRIPTS_DIR
from tests.unit.migrations.helpers.schema_migration_costants import (
    SchemaMigrationConstants,
)

substitution_variables = {"substitution_variables": "substitution_variables"}


def build(
    migration_scripts_folder_path: str = f"{TEST_SCRIPTS_DIR}.migration_scripts",
    migration_schema_name: str = SchemaMigrationConstants.schema_name,
    migration_table_name: str = SchemaMigrationConstants.table_name,
    table_prefix: str = "",
    catalog_name: str = "spark_catalog",
    substitutions: dict[str, str] | None = None,
) -> Configuration:
    if substitutions is None:
        substitutions = substitution_variables

    spark_config = spark_sql_migrations_configuration_builder.build(
        migration_scripts_folder_path=migration_scripts_folder_path,
        migration_schema_name=migration_schema_name,
        migration_table_name=migration_table_name,
        table_prefix=table_prefix,
        catalog_name=catalog_name,
        substitutions=substitutions,
    )

    return Configuration(spark_sql_migrations_configuration=spark_config)
