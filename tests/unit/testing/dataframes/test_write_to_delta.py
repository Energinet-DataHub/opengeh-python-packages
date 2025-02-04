from pyspark.sql import types as T

from geh_common.testing.dataframes import (
    assert_dataframes_and_schemas,
    write_to_delta,
)
from tests.unit.testing.scenario_testing.constants import SCENARIO_TESTING_TEST_CASE

schema_for_input = T.StructType(
    [
        T.StructField("a", T.StringType()),
        T.StructField("b", T.StringType()),
        T.StructField("c", T.StringType()),
    ]
)
schema_for_input_2 = T.StructType(
    [
        T.StructField("string", T.StringType()),
        T.StructField("boolean", T.BooleanType()),
        T.StructField("integer", T.IntegerType()),
    ]
)


def test_write_for_single_when_file(spark):
    # Actual
    file_name = "input.csv"
    files = [(file_name, schema_for_input)]
    write_to_delta.write_when_files_to_delta(
        spark=spark, scenario_path=str(SCENARIO_TESTING_TEST_CASE), files=files
    )
    actual_df = spark.sql(f"SELECT * FROM {file_name.removesuffix('.csv')}")

    # Expected
    expected_df = spark.createDataFrame([("hello", "world", "42")], schema_for_input)

    # Assert
    assert_dataframes_and_schemas(expected_df, actual_df)


def test_write_for_multiple_when_files(spark):
    # Setup
    file_name1 = "input.csv"
    file_name2 = "input2.csv"
    files = [
        (file_name1, schema_for_input),
        (file_name2, schema_for_input_2),
    ]

    # Actual
    write_to_delta.write_when_files_to_delta(
        spark=spark, scenario_path=str(SCENARIO_TESTING_TEST_CASE), files=files
    )
    actual1_df = spark.sql(f"SELECT * FROM {file_name1.removesuffix('.csv')}")
    actual2_df = spark.sql(f"SELECT * FROM {file_name2.removesuffix('.csv')}")

    # Expected
    expected1_df = spark.createDataFrame([("hello", "world", "42")], schema_for_input)
    expected2_df = spark.createDataFrame([("str", True, 42)], schema_for_input_2)

    # Assert
    assert_dataframes_and_schemas(actual=actual1_df, expected=expected1_df)
    assert_dataframes_and_schemas(actual=actual2_df, expected=expected2_df)


def test_write_to_delta_creates_table_with_same_name_as_file(spark):
    file_name = "input.csv"
    files = [(file_name, schema_for_input)]
    write_to_delta.write_when_files_to_delta(
        spark=spark, scenario_path=str(SCENARIO_TESTING_TEST_CASE), files=files
    )

    tables_df = spark.sql("SHOW TABLES")

    has_created_table = (
        tables_df.filter(tables_df.tableName == file_name.removesuffix(".csv")).count()
        > 0
    )

    assert has_created_table


def test_write_to_delta_overwrites_previous_data(spark):
    # Setup
    file_name = "input.csv"
    files = [(file_name, schema_for_input)]

    # Insert dummy data
    dummy_df = spark.createDataFrame(
        [("string1", "string2", "string3")], schema_for_input
    )
    dummy_df.write.mode("overwrite").format("delta").saveAsTable(
        file_name.removesuffix(".csv")
    )

    # Actual
    write_to_delta.write_when_files_to_delta(
        spark=spark, scenario_path=str(SCENARIO_TESTING_TEST_CASE), files=files
    )
    actual_df = spark.sql(f"SELECT * FROM {file_name.removesuffix('.csv')}")

    # Expected
    expected_df = spark.createDataFrame([("hello", "world", "42")], schema_for_input)

    # Assert
    assert_dataframes_and_schemas(expected_df, actual_df)


def test_file_is_ignored_if_it_does_not_exist_in_when(spark):
    """
    As the list of filename and schema tuples is often placed in a conftest and used across multiple scenarios,
    the `/when` files are not necessarily used for all scenarios.
    Therefore, the write_when_files_to_delta, should skip the files not used for the current scenario.
    """
    # Setup
    file_name1 = "input.csv"
    file_name2 = "input_for_another_scenario.csv"
    files = [(file_name1, schema_for_input), (file_name2, schema_for_input)]

    write_to_delta.write_when_files_to_delta(
        spark=spark, scenario_path=str(SCENARIO_TESTING_TEST_CASE), files=files
    )

    # Checks
    tables_df = spark.sql("SHOW TABLES")
    is_table_created_for_file_name1 = (
        tables_df.filter(tables_df.tableName == file_name1.removesuffix(".csv")).count()
        > 0
    )
    is_table_created_for_file_name2 = (
        tables_df.filter(tables_df.tableName == file_name2.removesuffix(".csv")).count()
        > 0
    )

    # Assert
    assert is_table_created_for_file_name1
    assert is_table_created_for_file_name2 is False
