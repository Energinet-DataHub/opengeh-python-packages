from dataclasses import dataclass


@dataclass
class SparkSqlMigrationsConfiguration:
    migration_schema_name: str
    """The name of the schema that contains the migration table"""
    migration_table_name: str
    """The name of the table that contains the migration scripts"""
    migration_scripts_folder_path: str
    """The folder path of the migration scripts"""
    substitution_variables: dict[str, str]
    """The substitution variables. These are used to replace variables in the migration scripts"""
    catalog_name: str
    """The name of the catalog"""
    table_prefix: str = ""
    """(Optional) A prefix to use for the table name"""
