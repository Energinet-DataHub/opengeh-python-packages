import shutil
import zipfile
from pathlib import Path

from geh_common.infrastructure.create_zip import create_zip_file
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
