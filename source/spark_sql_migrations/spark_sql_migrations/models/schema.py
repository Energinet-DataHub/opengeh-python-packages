from spark_sql_migrations.models.table import Table
from spark_sql_migrations.models.view import View


class Schema:
    def __init__(self, name: str, tables: list[Table], views: list[View]) -> None:
        self.name = name
        self.tables = tables
        self.views = views
