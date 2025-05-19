import zipfile
from pathlib import Path

from pyspark.sql import SparkSession

from geh_common.tasks.TaskBase import TaskBase
from geh_common.telemetry import Logger, use_span


class ZipTask(TaskBase):
    def __init__(self, spark: SparkSession, output_path: str | Path) -> None:
        """Initialize the ZipTask.

        Args:
            spark (SparkSession): The Spark session.
            dbutils (Any): The DBUtils object.
            output_path (str | Path): The path to the output directory.

        Raises:
            ValueError: If the output path is not a string or Path object.
        """
        super().__init__(spark)
        if not isinstance(output_path, str | Path):
            raise ValueError("Output path must be a string or Path object")
        self.log = Logger(self.__class__.__name__)
        self.output_path = output_path
        self.zip_output_path = f"{output_path}.zip"

    @use_span()
    def execute(self) -> None:
        """Create a zip file from the files in the output path."""
        files_to_zip = [f"{self.output_path}/{file_info.name}" for file_info in self.dbutils.fs.ls(self.output_path)]
        self.log.info(f"Files to zip: {files_to_zip}")
        self.log.info(f"Creating zip file: '{self.zip_output_path}'")
        self.create_zip_file(files_to_zip)
        self.log.info(f"Finished creating '{self.zip_output_path}'")

    @use_span()
    def create_zip_file(self, files_to_zip: list[str]) -> None:
        """Create a zip file from a list of files and saves it to the specified path.

        Notice that we have to create the zip file in /tmp and then move it to the desired
        location. This is done as `direct-append` or `non-sequential` writes are not
        supported in Databricks.

        Args:
            dbutils (Any): The DBUtils object.
            report_id (str): The report ID.
            save_path (str): The path to save the zip file.
            files_to_zip (list[str]): The list of files to zip.

        Raises:
            Exception: If there are no files to zip.
        """
        if len(files_to_zip) == 0:
            raise Exception("No files to zip")
        tmp_path = f"/tmp/{self.output_path}.zip"
        with zipfile.ZipFile(tmp_path, "a", zipfile.ZIP_DEFLATED) as ref:
            for fp in files_to_zip:
                file_name = fp.split("/")[-1]
                ref.write(fp, arcname=file_name)
        self.dbutils.fs.mv(f"file:{tmp_path}", self.zip_output_path)
