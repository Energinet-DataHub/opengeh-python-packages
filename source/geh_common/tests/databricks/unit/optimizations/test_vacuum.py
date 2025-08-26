from unittest import mock

import geh_common.databricks.optimizations.vacuum as sut


def test__vacuum_table__executes_expected_sql() -> None:
    # Arrange
    mocked_spark = mock.Mock()
    database = "test_database"
    table = "test_table"

    # Act
    sut.vacuum_table(mocked_spark, database, table)

    # Assert
    expected_sql = f"VACUUM {database}.{table}"
    mocked_spark.sql.assert_called_once_with(expected_sql)
