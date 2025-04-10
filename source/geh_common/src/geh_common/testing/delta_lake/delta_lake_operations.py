from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def create_database(spark: SparkSession, database_name: str) -> None:
    spark.sql(f"CREATE DATABASE {database_name}")


def create_table(
    spark: SparkSession,
    database_name: str,
    table_name: str,
    schema: StructType,
) -> None:
    sql_schema = _struct_type_to_sql_schema(schema)
    spark.sql(f"CREATE TABLE {database_name}.{table_name} ({sql_schema})")


def _struct_type_to_sql_schema(schema: StructType) -> str:
    schema_string = ""
    for field in schema.fields:
        field_name = field.name
        field_type = field.dataType.simpleString()

        if not field.nullable:
            field_type += " NOT NULL"

        schema_string += f"{field_name} {field_type}, "

    # Remove the trailing comma and space
    schema_string = schema_string.rstrip(", ")
    return schema_string
