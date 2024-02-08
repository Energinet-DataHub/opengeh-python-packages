import re
import glob
import package.schema_migration.schema_migration_pipeline as schema_migration_pipeline
import tests.helpers.mock_helper as mock_helper
from pyspark.sql import SparkSession
from unittest.mock import patch, Mock
from tests.helpers.path_helper import get_source_path


@patch.object(
    schema_migration_pipeline.apply_migrations.sql_file_executor.path_helper,
    schema_migration_pipeline.apply_migrations.sql_file_executor.path_helper.get_storage_base_path.__name__,
)
def test_dashboards(mock_path_helper: Mock, spark: SparkSession) -> None:
    # Arrange
    mock_path_helper.side_effect = mock_helper.base_path_helper
    schema_migration_pipeline.migrate()

    source_path = get_source_path()
    dashboards_path = f"{source_path}/MigrationTools/dashboards"
    queries = glob.glob(f"{dashboards_path}/*.dbdash")

    assert len(queries) > 0

    for q in queries:
        with open(q) as f:
            content = f.read()
            result = re.findall(r'"query"\s*:\s*"((?:\\"|[^"])*)"', content)

            for query in result:
                query = _replace_inputs(query)
                query = query.encode("utf-8").decode("unicode_escape")
                result = spark.sql(query)
                assert result is not None


def _replace_inputs(query: str) -> str:
    query = query.replace("{{ UseAllGridAreas }}", "\u0027True\u0027")
    query = query.replace("{{ GridAreaCode }}", "\u0027321\u0027")
    query = query.replace("{{ Grid area }}", "\u0027123\u0027")
    query = query.replace("{{ date }}", "\u002720-20-2020\u0027")
    query = query.replace("{{ from }}", "\u002720-20-2020\u0027")
    query = query.replace("{{ to }}", "\u002720-20-2020\u0027")
    query = query.replace("{{ Datetime_format }}", "\u002720-20-2020\u0027")
    return query
