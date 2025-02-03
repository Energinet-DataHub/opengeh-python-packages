import pyspark.sql.types as T

from opengeh_utilities.migrations.constants.migrations_constants import ColNames

schema_migration_schema = T.StructType(
    [
        T.StructField(ColNames.migration_name, T.StringType(), False),
        T.StructField(ColNames.execution_datetime, T.TimestampType(), False),
    ]
)
