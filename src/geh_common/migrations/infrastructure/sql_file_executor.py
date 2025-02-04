from importlib.resources import files

from dependency_injector.wiring import Provide, inject
from pyspark.sql import SparkSession

from opengeh_common.migrations.container import SparkSqlMigrationsContainer
from opengeh_common.migrations.models.configuration import Configuration


def execute(sql_file_name: str, folder_path: str) -> None:
    _execute(sql_file_name, folder_path)


@inject
def _execute(
    sql_file_name: str,
    folder_path: str,
    spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark],
) -> None:
    print(f"Executing SQL file '{sql_file_name}.sql'")  # noqa
    sql_content = files(folder_path).joinpath(f"{sql_file_name}.sql").read_text()

    queries = _split_string_by_go(sql_content)

    try:
        for query in queries:
            query = _substitute_placeholders(query)
            spark.sql(query)
    except Exception as exception:
        print(f"SQL file '{sql_file_name}.sql' failed with exception: {exception}")  # noqa
        raise exception


@inject
def _substitute_placeholders(query: str, config: Configuration = Provide[SparkSqlMigrationsContainer.config]) -> str:
    for key, value in config.substitution_variables.items():
        query = query.replace(key, value)

    return query


def _split_string_by_go(sql_content: str) -> list[str]:
    """Split SQL content by "GO" keyword.

    Databricks doesn't support multi-statement queries.
    So this emulates the "GO" used with SQL Server T-SQL.

    Args:
        sql_content (str): The SQL content to split.

    Returns:
        list[str]: A list of SQL statements.
    """
    lines = sql_content.replace("\r\n", "\n").split("\n")
    sections = []
    current_section: list[str] = []

    for line in lines:
        if "go" == line.lower().strip():
            if current_section:
                sections.append("\n".join(current_section))
                current_section = []
        else:
            current_section.append(line)

    if current_section:
        sections.append("\n".join(current_section))

    return [s for s in sections if s and not s.isspace()]
