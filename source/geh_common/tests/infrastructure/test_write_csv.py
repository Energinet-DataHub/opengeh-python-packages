import shutil
import string
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from geh_common.infrastructure.write_csv import (
    _get_partition_information,
    _write_dataframe,
    write_csv_files,
)


def test_write_csv_files__when_empty_dataframe__returns_empty_list(spark, tmp_path_factory):
    # Arrange
    report_output_dir = Path("test_write_csv_files__when_empty_dataframe__returns_empty_list")
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    df = spark.createDataFrame([], schema="id INT, value STRING")

    # Act
    new_files = write_csv_files(df, output_path=report_output_dir, tmpdir=tmpdir)

    # Assert
    assert len(new_files) == 1, f"Expected 1 new file to be created, but got {len(new_files)}"
    with open(new_files[0], "r") as f:
        content = f.read()
        assert content == "id,value\n", "Expected the file to be empty, but it is not"

    # Clean up
    shutil.rmtree(report_output_dir)
    shutil.rmtree(tmpdir)


def test_write_csv_files__with_file_name_factory__returns_expected_content(spark, tmp_path_factory):
    # Arrange
    report_output_dir = Path("test_write_csv_files__with_file_name_factory__returns_expected_content")
    spark_output_dir = report_output_dir / "spark_output"
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    expected_rows = 1_000
    rows = [(i, string.ascii_lowercase[i % 26]) for i in range(expected_rows)]
    expected_content = ["id,value"] + [f"{id},{value}" for id, value in rows]
    df = (
        spark.createDataFrame(rows, ["id", "value"]).orderBy("id").repartition(10)
    )  # Force multiple files to be created

    # Act
    new_files = write_csv_files(
        df,
        output_path=report_output_dir,
        spark_output_path=spark_output_dir,
        tmpdir=tmpdir,
        file_name_callback=lambda partitions: "test_csv",
    )

    # Assert
    n_spark_files = len(list(spark_output_dir.glob("*.csv")))
    assert n_spark_files > 1, f"Expected more than 1 Spark file to be created, but got {n_spark_files}"
    assert len(new_files) == 1, f"Expected 1 new file to be created, but got {len(new_files)}"
    assert new_files[0].exists(), f"File {new_files[0]} does not exist"
    assert new_files[0].stat().st_size > 0, f"File {new_files[0]} is empty"
    assert new_files[0].name == "test_csv.csv", f"Expected file name to be 'test_csv.csv', but got {new_files[0].name}"
    with open(new_files[0], "r") as f:
        actual_lines = f.read().splitlines()
        assert len(actual_lines) == expected_rows + 1, (
            f"Expected {expected_rows + 1:,} rows in the file, but got {len(actual_lines):,}"
        )

        actual_header = [actual_lines[0]]
        actual_body = sorted([(int(line.split(",")[0]), line.split(",")[1]) for line in actual_lines[1:]])
        actual_content = actual_header + [f"{id},{value}" for id, value in actual_body]
        assert actual_content == expected_content, "Expected content does not match actual content"

    # Clean up
    shutil.rmtree(report_output_dir)
    shutil.rmtree(tmpdir)


def test_write_csv_files__with_defaults__returns_expected(spark, tmp_path_factory):
    # Arrange
    report_output_dir = Path("test_write_csv_files__with_defaults__returns_expected")
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    df = spark.createDataFrame([(i, "a") for i in range(100_000)], ["id", "value"])

    # Act
    new_files = write_csv_files(df, output_path=report_output_dir, tmpdir=tmpdir)

    # Assert
    for f in new_files:
        assert f.exists(), f"File {f} does not exist"
        assert f.stat().st_size > 0, f"File {f} is empty"
        content: str = f.read_text()
        expected_rows = 100_000
        # accounting for header
        expected_rows += 1
        # When exactly divisible, we expect nrows to be equal to expected_rows. Otherwise, we expect it to be less
        assert len(content.splitlines()) == expected_rows, f"File {f} has more than {expected_rows} lines"

    # Clean up
    shutil.rmtree(report_output_dir)
    shutil.rmtree(tmpdir)


@pytest.mark.parametrize(
    "nrows, rows_per_file, expected_files",
    [
        (10, None, 1),
        (100, 10, 10),
        (1000, 200, 5),
        (10000, 3000, 4),
        (100, 3000, 1),
    ],
)
def test_write_csv_files__when_chunked__returns_expected_number_of_files(
    spark, tmp_path_factory, nrows, rows_per_file, expected_files
):
    # Arrange
    report_output_dir = tmp_path_factory.mktemp("test_write_csv_files__when_chunked__returns_expected_number_of_files")
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    df = spark.createDataFrame([(i, "a") for i in range(nrows)], ["id", "value"])

    # Act
    new_files = write_csv_files(df, output_path=report_output_dir, tmpdir=tmpdir, rows_per_file=rows_per_file)

    # Assert
    assert len(new_files) == expected_files, (
        f"Expected {expected_files} new files to be created, but got {len(new_files)}"
    )

    for f in new_files:
        assert f.exists(), f"File {f} does not exist"
        assert f.stat().st_size > 0, f"File {f} is empty"
        content: str = f.read_text()
        expected_rows = rows_per_file or nrows
        # When exactly divisible, we expect nrows to be equal to expected_rows. Otherwise, we expect it to be less
        if nrows % expected_rows == 0:
            # accounting for header
            expected_rows += 1
            assert len(content.splitlines()) == expected_rows, f"File {f} has more than {rows_per_file} lines"
        else:
            # accounting for header
            expected_rows += 1
            assert len(content.splitlines()) <= expected_rows, f"File {f} has more than {rows_per_file} lines"

    # Clean up
    shutil.rmtree(tmpdir)
    shutil.rmtree(report_output_dir)


@pytest.mark.parametrize(
    "nrows, rows_per_file, expected_files",
    [
        (10, None, 1),
        (100, 10, 10),
        (1000, 200, 5),
        (10000, 3000, 4),
    ],
)
def test_write_csv_files__when_chunked_with_custom_names__returns_n_files_with_custom_name(
    spark, tmp_path_factory, nrows, rows_per_file, expected_files
):
    # Arrange
    report_output_dir = tmp_path_factory.mktemp(
        "test_write_csv_files__when_chunked_with_custom_names__returns_n_files_with_custom_name"
    )
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    df = spark.createDataFrame([(i, "a") for i in range(nrows)], ["id", "value"])

    class MyFileNameFactory(FileNameFactoryBase):
        def create(self, partitions: dict[str, str]) -> str:
            return "my_file"

    # Act
    new_files = write_csv_files(
        df,
        output_path=report_output_dir,
        tmpdir=tmpdir,
        rows_per_file=rows_per_file,
        file_name_callback=MyFileNameFactory(),
    )

    # Assert
    for f in new_files:
        assert f.exists(), f"File {f} does not exist"
        assert f.stat().st_size > 0, f"File {f} is empty"
        content: str = f.read_text()
        expected_rows = rows_per_file or nrows
        # accounting for header
        expected_rows += 1
        # When exactly divisible, we expect nrows to be equal to expected_rows. Otherwise, we expect it to be less
        if nrows % expected_rows == 0:
            assert len(content.splitlines()) == expected_rows, f"File {f} has more than {rows_per_file} lines"
        else:
            assert len(content.splitlines()) <= expected_rows, f"File {f} has more than {rows_per_file} lines"

    # Clean up
    shutil.rmtree(tmpdir)
    shutil.rmtree(report_output_dir)


@pytest.mark.parametrize(
    "input_path, expected",
    [
        ("/tmp/test", {}),
        (Path("/tmp/test"), {}),
        ("/tmp/part=1/test", {"part": "1"}),
        ("/tmp/part=1/part2=2/test", {"part": "1", "part2": "2"}),
        ("/tmp/part=1/part2=2/part3=3/continued/path/to/test", {"part": "1", "part2": "2", "part3": "3"}),
    ],
)
def test_get_partitions__when_valid__returns_partitions(input_path, expected):
    """Test the get_partitions function."""
    # Call the function and assert the result
    assert _get_partition_information(input_path) == expected


@pytest.mark.parametrize(
    "input_path, error_type, matchstmt",
    [
        ("/tmp/part=1/part2=2/part3=3=5", ValueError, "too many values to unpack"),
    ],
)
def test_get_partitions__when_invalid__throws_exception(input_path, error_type, matchstmt):
    """Test the get_partitions function with invalid input."""
    with pytest.raises(error_type, match=matchstmt):
        _get_partition_information(input_path)


def test_write_files__csv_separator_is_comma_and_decimals_use_points(
    spark: SparkSession,
    tmp_path_factory,
):
    # Arrange
    df = spark.createDataFrame([("a", 1.1), ("b", 2.2), ("c", 3.3)], ["key", "value"])
    tmp_dir = tmp_path_factory.mktemp("test_zip_task")
    csv_path = f"{tmp_dir}/csv_file"

    # Act
    columns = _write_dataframe(
        df,
        csv_path,
        partition_columns=[],
        order_by=[],
        rows_per_file=1000,
    )

    # Assert
    assert Path(csv_path).exists()

    for x in Path(csv_path).iterdir():
        if x.is_file() and x.name[-4:] == ".csv":
            with x.open(mode="r") as f:
                all_lines_written = f.readlines()

                assert all_lines_written[0] == "a,1.1\n"
                assert all_lines_written[1] == "b,2.2\n"
                assert all_lines_written[2] == "c,3.3\n"

    assert columns == ["key", "value"]

    # Clean up
    shutil.rmtree(tmp_dir)


def test_write_files__when_order_by_specified_on_multiple_partitions(
    spark: SparkSession,
    tmp_path_factory,
):
    # Arrange
    df = spark.createDataFrame(
        [("b", 2.2), ("b", 1.1), ("c", 3.3)],
        ["key", "value"],
    )
    tmp_dir = tmp_path_factory.mktemp("test_zip_task")
    csv_path = f"{tmp_dir}/csv_file"

    # Act
    columns = _write_dataframe(
        df,
        csv_path,
        partition_columns=["key"],
        order_by=["value"],
        rows_per_file=1000,
    )

    # Assert
    assert Path(csv_path).exists()

    for p in Path(csv_path).iterdir():
        if p.is_file() and p.suffix == ".csv":
            with p.open(mode="r") as f:
                all_lines_written = f.readlines()

                if len(all_lines_written) == 1:
                    assert all_lines_written[0] == "c,3.3\n"
                elif len(all_lines_written) == 2:
                    assert all_lines_written[0] == "b,1.1\n"
                    assert all_lines_written[1] == "b,2.2\n"
                else:
                    raise AssertionError("Found unexpected csv file.")

    assert columns == ["value"]

    # Clean up
    shutil.rmtree(tmp_dir)


def test_write_files__when_df_includes_timestamps__creates_csv_without_milliseconds(
    spark: SparkSession,
    tmp_path_factory,
):
    # Arrange
    df = spark.createDataFrame(
        [
            ("a", datetime(2024, 10, 21, 12, 10, 30, 0, tzinfo=timezone.utc)),
            ("b", datetime(2024, 10, 21, 12, 10, 30, 30, tzinfo=timezone.utc)),
            ("c", datetime(2024, 10, 21, 12, 10, 30, 123, tzinfo=timezone.utc)),
        ],
        ["key", "value"],
    )
    tmp_dir = tmp_path_factory.mktemp("test_zip_task")
    csv_path = f"{tmp_dir}/csv_file"

    # Act
    columns = _write_dataframe(
        df,
        csv_path,
        partition_columns=[],
        order_by=[],
        rows_per_file=1000,
    )

    # Assert
    assert Path(csv_path).exists()

    for p in Path(csv_path).iterdir():
        if p.is_file() and p.suffix == ".csv":
            with p.open(mode="r") as f:
                all_lines_written = f.readlines()

                assert all_lines_written[0] == "a,2024-10-21T12:10:30Z\n"
                assert all_lines_written[1] == "b,2024-10-21T12:10:30Z\n"
                assert all_lines_written[2] == "c,2024-10-21T12:10:30Z\n"

    assert columns == ["key", "value"]

    # Clean up
    shutil.rmtree(tmp_dir)
