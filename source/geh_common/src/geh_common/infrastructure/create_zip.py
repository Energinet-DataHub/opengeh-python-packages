import zipfile
from pathlib import Path
from typing import Any


def create_zip_file(path: str | Path, dbutils: Any, tmpdir: str | Path = Path("tmp")) -> Path:
    """Create a zip file from a list of files and saves it to the specified path.

    Notice that we have to create the zip file in /tmp and then move it to the desired
    location. This is done as `direct-append` or `non-sequential` writes are not
    supported in Databricks.

    Args:
        path (str | Path): The path to the directory containing the files to zip.
        dbutils: The Databricks utilities object.
        tmpdir (str | Path, optional): The temporary directory to write the zip file to. Defaults to "tmp".

    Raises:
        Exception: If there are no files to zip.

    Returns:
        Path: The path to the created zip file.
    """
    output_path = Path(path)
    if not output_path.exists():
        raise ValueError(f"'{output_path}' does not exist")
    if not output_path.is_dir():
        raise ValueError(f"'{output_path}' is not a directory")
    zip_output_path = output_path.with_suffix(".zip")
    files_to_zip = [Path(f) for f in dbutils.fs.ls(str(output_path))]
    if len(files_to_zip) == 0:
        raise FileNotFoundError(f"No files found in {output_path}")
    tmp_path = Path(tmpdir) / zip_output_path.name
    Path(tmp_path).parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as ref:
        for fp in files_to_zip:
            file_name = fp.name
            ref.write(fp, arcname=file_name)
    dbutils.fs.mv(f"file:{tmp_path}", zip_output_path)
    return zip_output_path
