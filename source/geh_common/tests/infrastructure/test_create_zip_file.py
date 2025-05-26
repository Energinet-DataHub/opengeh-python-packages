import shutil
import zipfile
from pathlib import Path

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
