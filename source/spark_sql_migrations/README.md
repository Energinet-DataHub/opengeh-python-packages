# Spark SQL Migrations

## Usage

```python

from spark_sql_migrations import (
    create_and_configure_container,
    schema_migration_pipeline,
    SparkSqlMigrationsConfiguration,
    Table,
    Schema
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


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
    migration_schema_name="schema_name",
    migration_schema_location="schema_location",
    migration_table_name="table_name",
    migration_table_location="table_location",
    migration_scripts_folder_path="migration_scripts_folder_path",
    table_prefix="table_prefix",
    current_state_schemas_folder_path="current_state_schemas_folder_path",
    current_state_tables_folder_path="current_state_tables_folder_path",
    schema_config=schema_config,
    substitution_variables=substitutions,
)

create_and_configure_container(spark_config)
schema_migration_pipeline.migrate()


```

## Current State

The purpose of the `current step` concept is to be able to create schemas and tables that might have been
deleted for some reason, even though they have been created by the normal migration process.

In the `SparkSqlMigrationsConfiguration` class, there are two fields that are used to define the folder path of the
schemas and tables that are part of the `current step`:

- `current_state_schemas_folder_path`: The folder path to SQL scripts that creates all schemas.
- `current_state_tables_folder_path`: The folder path to SQL scripts that creates all tables.

The `current step` is executed when there are missing schemas or tables in the Catalog, based on the `schema_config` field in the
`SparkSqlMigrationsConfiguration` class.
