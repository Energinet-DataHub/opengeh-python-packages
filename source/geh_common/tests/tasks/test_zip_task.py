import shutil
from pathlib import Path

import pytest

from geh_common.tasks.ZipTask import ZipTask


@pytest.fixture
def mock_dbutils(monkeypatch):
    class MockFileInfo:
        def __init__(self, name):
            self.name = name

    class MockDBUtils:
        def fs(self):
            class MockFS:
                def ls(self, path):
                    return [MockFileInfo(f.name) for f in Path(path).iterdir()]

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
    assert task.dbutils.fs().ls(tmp_path) == []
    assert isinstance(task.dbutils, mock_dbutils.__class__)
    for i in range(3):
        (tmp_path / f"file_{i}.txt").write_text(f"Content of file {i}")

    # Act
    files: list[Path] = task.dbutils.fs().ls(str(tmp_path))

    # Assert
    assert len(files) == 3
    assert sorted([f.name for f in files]) == sorted([f"file_{i}.txt" for i in range(3)])

    # Clean up
    shutil.rmtree(tmp_path)
