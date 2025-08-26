from unittest import mock

import geh_common.databricks.optimizations.analyze as sut


def test__analyze_table__when_analyze_columns_not_parsed__executes_expected_sql() -> None:
    # Arrange
    mocked_spark = mock.Mock()
    database = "test_database"
    table = "test_table"

    # Act
    sut.analyze_table(mocked_spark, database, table)

    # Assert
    expected_sql = f"ANALYZE TABLE {database}.{table} COMPUTE STATISTICS FOR ALL COLUMNS"
    mocked_spark.sql.assert_called_once_with(expected_sql)


def test__analyze_table__when_analyze_columns_parsed__executes_expected_sql() -> None:
    # Arrange
    mocked_spark = mock.Mock()
    database = "test_database"
    table = "test_table"
    analyze_columns = "column1, column2"

    # Act
    sut.analyze_table(mocked_spark, database, table, analyze_columns)

    # Assert
    expected_sql = f"ANALYZE TABLE {database}.{table} COMPUTE STATISTICS FOR COLUMNS {analyze_columns}"
    mocked_spark.sql.assert_called_once_with(expected_sql)
