import shutil
from pathlib import Path

import pytest

from geh_common.tasks.ZipTask import ZipTask, write_csv_files


@pytest.fixture
def mock_dbutils(monkeypatch):
    class MockFileInfo:
        def __init__(self, name):
            self.name = name

    class MockDBUtils:
        @property
        def fs(self):
            class MockFS:
                def ls(self, path):
                    return [MockFileInfo(f.name) for f in Path(path).iterdir()]

                def mv(self, src: str | Path, dst: str | Path):
                    src = str(src)
                    dst = str(dst)
                    if src.startswith("/dbfs/") or dst.startswith("/dbfs/"):
                        src = src.replace("/dbfs/", "")
                        dst = dst.replace("/dbfs/", "")
                    if src.startswith("dbfs:/") or dst.startswith("dbfs:/"):
                        src = src.replace("dbfs:/", "")
                        dst = dst.replace("dbfs:/", "")
                    if src.startswith("file:/") or dst.startswith("file:/"):
                        src = src.replace("file:/", "")
                        dst = dst.replace("file:/", "")
                    shutil.move(Path(src), Path(dst))

                def cp(self, src, dst):
                    if Path(src).is_dir():
                        shutil.copytree(src, dst)
                    else:
                        shutil.copy(src, dst)

            return MockFS()

    monkeypatch.setattr("geh_common.tasks.TaskBase.get_dbutils", lambda _: MockDBUtils())

    return MockDBUtils()


def test_init_zip_task(spark, mock_dbutils):
    task = ZipTask(spark, "/tmp/test")
    assert task.output_path == "/tmp/test"
    assert task.zip_output_path == "/tmp/test.zip"
    assert task.spark == spark


def test_dbutils_mocked(spark, tmp_path_factory, mock_dbutils):
    # Arrange
    tmp_path: Path = tmp_path_factory.mktemp("test_zip_task")
    task = ZipTask(spark, tmp_path)
    assert task.dbutils.fs.ls(tmp_path) == []
    assert isinstance(task.dbutils, mock_dbutils.__class__)
    for i in range(3):
        (tmp_path / f"file_{i}.txt").write_text(f"Content of file {i}")

    # Act
    files: list[Path] = task.dbutils.fs.ls(str(tmp_path))

    # Assert
    assert len(files) == 3
    assert sorted([f.name for f in files]) == sorted([f"file_{i}.txt" for i in range(3)])

    # Clean up
    shutil.rmtree(tmp_path)


@pytest.mark.parametrize(
    "nrows, rows_per_file, expected_files",
    [
        (10, None, 1),
        (100, 10, 10),
        (1000, 200, 5),
        (10000, 3000, 4),
    ],
)
def test_zip_task_write_files_in_chunks(spark, tmp_path_factory, nrows, rows_per_file, expected_files, mock_dbutils):
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
    "nrows, rows_per_file, expected_files",
    [
        (10, None, 1),
        (100, 10, 10),
        (1000, 200, 5),
        (10000, 3000, 4),
    ],
)
def test_zip_task_write_files_in_chunks_with_custom_file_names(
    spark, tmp_path_factory, nrows, rows_per_file, expected_files, mock_dbutils
):
    # Arrange
    report_output_dir = tmp_path_factory.mktemp("test_zip_task")
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    output_path = report_output_dir / "test_file.txt"
    df = spark.createDataFrame([(i, "a") for i in range(nrows)], ["id", "value"])

    custom_prefix = "custom_chunk"

    def file_name_factory(file_name: str) -> str:
        return file_name.replace("chunk", custom_prefix)

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
