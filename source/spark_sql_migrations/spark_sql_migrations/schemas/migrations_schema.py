from spark_sql_migrations.constants.migrations_constants import ColNames
from pyspark.sql.types import (
    StructField,
    StringType,
    StructType,
    TimestampType
)

schema_migration_schema = StructType(
    [
        StructField(ColNames.migration_name, StringType(), False),
        StructField(ColNames.execution_datetime, TimestampType(), False)
    ]
)
