import random
import string
import zipfile
from pathlib import Path
from typing import Any


def create_zip_file(dbutils: Any, save_path: str | Path, files_to_zip: list[str]) -> Path:
    """Create a zip file from a list of files and saves it to the specified path.

    Notice that we have to create the zip file in /tmp and then move it to the desired
    location. This is done as `direct-append` or `non-sequential` writes are not
    supported in Databricks.

    Args:
        dbutils (Any): The Databricks utilities object to handle file operations.
        save_path (str | Path): The path where the zip file will be saved.
        files_to_zip (list[str]): A list of file paths to include in the zip file.

    Raises:
        Exception: If there are no files to zip.

    Returns:
        Path: The path to the created zip file.
    """
    if len(files_to_zip) == 0:
        raise ValueError("No files to zip provided")
    elif not str(save_path).endswith(".zip"):
        raise ValueError("The save path must end with '.zip'")

    random_str = "".join(random.choices(string.ascii_lowercase, k=10))
    tmp_path = f"/tmp/zip_{random_str}.zip"
    Path(tmp_path).parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as ref:
        for file_path in files_to_zip:
            file_name = Path(file_path).name
            ref.write(file_path, arcname=file_name)
    dbutils.fs.mv(f"file:{tmp_path}", str(save_path))
    return Path(save_path)
