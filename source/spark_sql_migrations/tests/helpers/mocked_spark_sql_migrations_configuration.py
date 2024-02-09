from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from spark_sql_migrations.models.table import Table
from spark_sql_migrations.models.schema import Schema
from spark_sql_migrations.models.spark_sql_migrations_configuration import SparkSqlMigrationsConfiguration


schema = StructType(
    [
        StructField("column1", StringType(), False),
        StructField("column2", StringType(), False)
    ]
)

schema_config = [Schema(name="test_schema", tables=[Table(name="test_table", schema=schema)])]

config = SparkSqlMigrationsConfiguration(
    current_state_schemas_folder_path="tests.test_scripts.schema_scripts",
    current_state_tables_folder_path="tests.test_scripts.table_scripts",
    migration_scripts_folder_path="tests.test_scripts.migration_scripts",
    migration_schema_name="migration_schema_name",
    migration_table_name="migration_table_name",
    table_prefix="table_prefix",
    migration_schema_location="migration_schema_location",
    migration_table_location="migration_table_location",
    db_folder="",
    schema_config=schema_config,
    substitution_variables={"substitution_variables": "substitution_variables"},
)
