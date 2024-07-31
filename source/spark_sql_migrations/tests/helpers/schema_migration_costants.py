class SchemaMigrationConstants:
    catalog_name = "spark_catalog"
    schema_name = "schema_migration"
    table_name = "executed_migrations"
    migration_scripts_folder_path = "package.schema_migration.migration_scripts"
    current_state_schemas_folder_path = (
        "package.schema_migration.current_state_scripts.schemas"
    )
    current_state_tables_folder_path = (
        "package.schema_migration.current_state_scripts.tables"
    )
    current_state_views_folder_path = (
        "package.schema_migration.current_state_scripts.views"
    )
