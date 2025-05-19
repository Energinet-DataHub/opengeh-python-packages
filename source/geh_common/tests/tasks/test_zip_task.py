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
                    return [MockFileInfo("file1.txt"), MockFileInfo("file2.txt")]

            return MockFS()

    monkeypatch.setattr("geh_common.tasks.TaskBase.get_dbutils", lambda _: MockDBUtils())

    return MockDBUtils()


def test_init_zip_task(spark, mock_dbutils):
    task = ZipTask(spark, "/tmp/test")
    assert task.output_path == "/tmp/test"
    assert task.zip_output_path == "/tmp/test.zip"
    assert task.spark == spark
