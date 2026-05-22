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


def test__optimize_table_zorder__executes_expected_sql() -> None:
    # Arrange
    mocked_spark = mock.Mock()
    database = "test_database"
    table = "test_table"
    zorder_columns = ["col_a", "col_b"]

    # Act
    sut.optimize_table_zorder(mocked_spark, database, table, zorder_columns)

    # Assert
    expected_sql = f"OPTIMIZE {database}.{table} ZORDER BY (col_a, col_b)"
    mocked_spark.sql.assert_called_once_with(expected_sql)


def test__optimize_table_zorder__with_single_column__executes_expected_sql() -> None:
    # Arrange
    mocked_spark = mock.Mock()
    database = "test_database"
    table = "test_table"
    zorder_columns = ["col_a"]

    # Act
    sut.optimize_table_zorder(mocked_spark, database, table, zorder_columns)

    # Assert
    expected_sql = f"OPTIMIZE {database}.{table} ZORDER BY (col_a)"
    mocked_spark.sql.assert_called_once_with(expected_sql)


def test__optimize_table_zorder__with_where_clause__executes_expected_sql() -> None:
    # Arrange
    mocked_spark = mock.Mock()
    database = "test_database"
    table = "test_table"
    zorder_columns = ["col_a", "col_b"]
    where = "date >= '2026-01-01'"

    # Act
    sut.optimize_table_zorder(mocked_spark, database, table, zorder_columns, where=where)

    # Assert
    expected_sql = f"OPTIMIZE {database}.{table} WHERE {where} ZORDER BY (col_a, col_b)"
    mocked_spark.sql.assert_called_once_with(expected_sql)
