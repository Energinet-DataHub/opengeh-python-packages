import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from geh_common.tasks.ZipTask import (
    CHUNK_INDEX_COLUMN,
    _write_dataframe,
    create_zip_file,
    get_partitions,
    write_csv_files,
)
from geh_common.testing.spark.mocks import MockDBUtils


def test_zip_dir(tmp_path_factory):
    # Arrange
    outputdir = tmp_path_factory.mktemp("test_zip_task")
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    files = ["file_1.txt", "file_2.txt", "file_3.txt"]
    for file in files:
        (outputdir / file).write_text(f"Content of {file}")

    # Act
    zip_file = create_zip_file(outputdir, MockDBUtils(), tmpdir)

    # Assert
    assert zip_file == Path(outputdir).with_suffix(".zip"), (
        f"Expected {zip_file} to be {Path(outputdir).with_suffix('.zip')}"
    )
    assert zip_file.exists(), f"Zip file {zip_file} does not exist"
    assert zip_file.stat().st_size > 0, f"Zip file {zip_file} is empty"

    with zipfile.ZipFile(zip_file, "r") as zf:
        zip_files = zf.namelist()
        assert len(zip_files) == len(files), f"Expected {len(files)} files in zip, but got {len(zip_files)}"
        for file in files:
            assert file in zip_files, f"File {file} not found in zip"

    # Clean up
    shutil.rmtree(outputdir)
    shutil.rmtree(tmpdir)


def test_zip_task_write_files_default(spark):
    # Arrange
    report_output_dir = Path("test_zip_task")
    output_path = report_output_dir / "test_file.txt"
    df = spark.createDataFrame([(i, "a") for i in range(100_000)], ["id", "value"])

    # Act
    new_files = write_csv_files(df, output_path=report_output_dir)

    # Assert
    file_list = "- " + "\n- ".join(list([str(f) for f in output_path.rglob("*")]))
    for i, f in enumerate(sorted(list(output_path.rglob("*")))):
        assert f.is_file(), f"File {f} is not a file"
        assert f.name.endswith(".csv"), f"File {f} is not a csv file"

    assert len(new_files) == 1, f"Expected {1} new files to be created, but got\n{file_list}"

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
def test_zip_task_write_files_in_chunks(spark, tmp_path_factory, nrows, rows_per_file, expected_files):
    # Arrange
    report_output_dir = tmp_path_factory.mktemp("test_zip_task")
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    output_path = report_output_dir / "test_file.txt"
    df = spark.createDataFrame([(i, "a") for i in range(nrows)], ["id", "value"])

    # Act
    new_files = write_csv_files(df, output_path=report_output_dir, tmpdir=tmpdir, rows_per_file=rows_per_file)

    # Assert
    file_list = "- " + "\n- ".join(list([str(f) for f in output_path.rglob("*")]))
    for i, f in enumerate(sorted(list(output_path.rglob("*")))):
        assert f.is_file(), f"File {f} is not a file"
        assert f.name == f"chunk_{i}", f"File {f} is not named chunk_{i}"
        assert f.name.endswith(".csv"), f"File {f} is not a csv file"

    assert len(new_files) == expected_files, f"Expected {expected_files} new files to be created, but got\n{file_list}"
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
def test_zip_task_write_files_in_chunks_with_custom_file_names(
    spark, tmp_path_factory, nrows, rows_per_file, expected_files
):
    # Arrange
    report_output_dir = tmp_path_factory.mktemp("test_zip_task")
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    output_path = report_output_dir / "test_file.txt"
    df = spark.createDataFrame([(i, "a") for i in range(nrows)], ["id", "value"])

    custom_prefix = "custom_chunk"

    def file_name_factory(_, partitions: dict[str, str]) -> str:
        chunk_index = partitions.get(CHUNK_INDEX_COLUMN)
        return f"{custom_prefix}_{chunk_index}.csv"

    # Act
    new_files = write_csv_files(
        df,
        output_path=report_output_dir,
        tmpdir=tmpdir,
        rows_per_file=rows_per_file,
        file_name_factory=file_name_factory,
    )

    # Assert
    file_list = "- " + "\n- ".join(list([str(f) for f in output_path.rglob("*")]))
    for i, f in enumerate(sorted(list(output_path.rglob("*")))):
        assert f.is_file(), f"File {f} is not a file"
        assert f.name == f"{custom_prefix}_{i}", f"File {f} is not named {custom_prefix}_{i}"
        assert f.name.endswith(".csv"), f"File {f} is not a csv file"

    assert len(new_files) == expected_files, f"Expected {expected_files} new files to be created, but got\n{file_list}"
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
def test_get_partitions(input_path, expected):
    """Test the get_partitions function."""
    # Call the function and assert the result
    assert get_partitions(input_path) == expected


@pytest.mark.parametrize(
    "input_path, error_type, matchstmt",
    [
        ("/tmp/part=1/part2=2/part3=3=5", ValueError, "too many values to unpack"),
    ],
)
def test_get_partitions_invalid(input_path, error_type, matchstmt):
    """Test the get_partitions function with invalid input."""
    with pytest.raises(error_type, match=matchstmt):
        get_partitions(input_path)


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

    shutil.rmtree(csv_path)


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
    csv_path = f"{tmp_dir.name}/csv_file"

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

    shutil.rmtree(csv_path)


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
    csv_path = f"{tmp_dir.name}/csv_file"

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

    shutil.rmtree(csv_path)
