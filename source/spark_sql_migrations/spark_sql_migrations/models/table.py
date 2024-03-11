from pyspark.sql.types import StructType


class Table:
    def __init__(self, name: str, schema: StructType) -> None:
        self.name = name
        self.schema = schema
