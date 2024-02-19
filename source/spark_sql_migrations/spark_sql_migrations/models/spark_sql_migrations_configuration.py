from dataclasses import dataclass


@dataclass
class SparkSqlMigrationsConfiguration:
    migration_schema_name: str
    """The name of the schema that contains the migration table"""
    migration_schema_location: str
    """The location of the schema that contains the migration table"""
    migration_table_name: str
    """The name of the table that contains the migration scripts"""
    migration_table_location: str
    """The location of the table that contains the migration scripts"""
    migration_scripts_folder_path: str
    """The folder path of the migration scripts"""
    current_state_schemas_folder_path: str
    """The folder path to the schema files"""
    current_state_tables_folder_path: str
    """The folder path to the table files"""
    schema_config: any
    """The schema configuration, telling the migration tool which schemas and tables to check."""
    substitution_variables: dict[str, str]
    """The substitution variables. These are used to replace variables in the migration scripts"""
    db_folder: str = ""
    """(Optional) An extra folder to use for the database"""
    table_prefix: str = ""
    """(Optional) A prefix to use for the table name"""
