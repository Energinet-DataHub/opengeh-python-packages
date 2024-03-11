from spark_sql_migrations.models.table import Table


class Schema:
    def __init__(self, name: str, tables: list[Table]) -> None:
        self.name = name
        self.tables = tables
