# Spark SQL Migrations

## Usage

```python

import spark_sql_migrations.migrations.schema_migration_pipeline as schema_migration_pipeline
from spark_sql_migrations.container import create_and_configure_container
from spark_sql_migrations.models.spark_sql_migrations_configuration import (
    SparkSqlMigrationsConfiguration,
)
from spark_sql_migrations.models.table import Table
from spark_sql_migrations.models.schema import Schema

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

def migrate() -> None:
    _configure_spark_sql_migration()
    _migrate()


def _configure_spark_sql_migration() -> None:
    schema = StructType(
        [
            StructField("column1", StringType(), False),
            StructField("column2", StringType(), False)
        ]
    )

    schema_config = [
        Schema(
            name="test_schema",
            tables=[
                Table(name="test_table", schema=schema),
                Table(name="test_table_2", schema=schema)
            ]
        )
    ]

    substitutions = {"{location}": "some_location"}

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name= "schema_name",
        migration_schema_location="schema_location",
        migration_table_name="table_name",
        migration_table_location="table_location",
        migration_scripts_folder_path="migration_scripts_folder_path",
        db_folder="db_folder",
        table_prefix="table_prefix",
        current_state_schemas_folder_path="current_state_schemas_folder_path",
        current_state_tables_folder_path="current_state_tables_folder_path",
        schema_config=schema_config,
        substitution_variables=substitutions,
    )

    create_and_configure_container(spark_config)


def _migrate() -> None:
    schema_migration_pipeline.migrate()


```
