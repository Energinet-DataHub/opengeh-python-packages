import os
import glob
import json
import package.schema_migration.schema_migration_pipeline as schema_migration_pipeline
import tests.helpers.mock_helper as mock_helper
from pyspark.sql import SparkSession
from unittest.mock import patch, Mock
from tests.helpers.path_helper import get_source_path


@patch.object(
    schema_migration_pipeline.apply_migrations.sql_file_executor.path_helper,
    schema_migration_pipeline.apply_migrations.sql_file_executor.path_helper.get_storage_base_path.__name__,
)
def test_queries(mock_path_helper: Mock, spark: SparkSession) -> None:
    # Arrange
    mock_path_helper.side_effect = mock_helper.base_path_helper
    schema_migration_pipeline.migrate()

    source_path = get_source_path()
    queries_folder_path = f"{source_path}/MigrationTools/queries"
    queries_path = os.path.abspath(queries_folder_path)

    queries = glob.glob(f"{queries_path}/*.json")

    assert len(queries) > 0

    for q in queries:
        with open(q) as f:
            query = json.load(f)
            actual_query = query["query"]
            result = spark.sql(actual_query)
            assert result is not None
