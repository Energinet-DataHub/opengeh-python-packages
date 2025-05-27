import shutil
import zipfile
from pathlib import Path

import pytest

from geh_common.infrastructure.create_zip import create_zip_file
from geh_common.testing.spark.mocks import MockDBUtils


def test_zip_dir(tmp_path_factory):
    # Arrange
    outputdir = tmp_path_factory.mktemp("test_zip_task")
    tmpdir = tmp_path_factory.mktemp("tmp_dir")
    files = [str(tmpdir / "file_1.txt"), str(tmpdir / "file_2.txt"), str(tmpdir / "file_3.txt")]
    for f in files:
        Path(f).write_text(f"Content of {f}")

    # Act
    zip_file = create_zip_file(MockDBUtils(), outputdir.with_suffix(".zip"), files)

    # Assert
    assert zip_file == Path(outputdir).with_suffix(".zip"), (
        f"Expected {zip_file} to be {Path(outputdir).with_suffix('.zip')}"
    )
    assert zip_file.exists(), f"Zip file {zip_file} does not exist"
    assert zip_file.stat().st_size > 0, f"Zip file {zip_file} is empty"

    with zipfile.ZipFile(zip_file, "r") as zf:
        zip_files = zf.namelist()
        assert len(zip_files) == len(files), f"Expected {len(files)} files in zip, but got {len(zip_files)}"
        for f in files:
            assert Path(f).name in zip_files, f"File {f} not found in zip"

    # Clean up
    shutil.rmtree(outputdir)
    shutil.rmtree(tmpdir)


def test_create_zip_file__when_dbutils_is_none__raise_exception():
    # Arrange
    dbutils = None
    save_path = "save_path.zip"
    files_to_zip = ["file1", "file2"]

    # Act
    with pytest.raises(Exception):
        create_zip_file(dbutils, save_path, files_to_zip)


def test_create_zip_file__when_save_path_is_not_zip__raise_exception():
    # Arrange
    dbutils = None
    save_path = "save_path"
    files_to_zip = ["file1", "file2"]

    # Act
    with pytest.raises(Exception):
        create_zip_file(dbutils, save_path, files_to_zip)


def test_create_zip_file__when_no_files_to_zip__raise_exception():
    # Arrange
    dbutils = None
    save_path = "save_path.zip"
    files_to_zip = ["file1", "file2"]

    # Act
    with pytest.raises(Exception):
        create_zip_file(dbutils, save_path, files_to_zip)


def test_create_zip_file__when_files_to_zip__create_zip_file(tmp_path_factory):
    # Arrange
    tmp_dir = tmp_path_factory.mktemp("tmp_dir")
    save_path = tmp_dir / "test_zip_file.zip"
    files_to_zip = [str(tmp_dir / f"file_{i}.txt") for i in range(3)]
    for file_path in files_to_zip:
        Path(file_path).write_text(f"Content of {file_path}")

    # Act
    create_zip_file(MockDBUtils(), save_path, files_to_zip)

    # Assert
    assert Path(save_path).exists()

    # Clean up
    shutil.rmtree(tmp_dir)
