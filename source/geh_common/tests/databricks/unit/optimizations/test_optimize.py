from unittest import mock

import geh_common.databricks.optimizations.optimize as sut


def test__optimize_table__executes_expected_sql() -> None:
    # Arrange
    mocked_spark = mock.Mock()
    database = "test_database"
    table = "test_table"

    # Act
    sut.optimize_table(mocked_spark, database, table)

    # Assert
    expected_sql = f"OPTIMIZE {database}.{table}"
    mocked_spark.sql.assert_called_once_with(expected_sql)
