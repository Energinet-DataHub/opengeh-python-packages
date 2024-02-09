from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from spark_sql_migrations.models.table import Table
from spark_sql_migrations.models.schema import Schema


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
