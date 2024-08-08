from pyspark.sql.types import StructType


class View:
    def __init__(self, name: str, schema: StructType):
        self.name = name
        self.schema = schema
