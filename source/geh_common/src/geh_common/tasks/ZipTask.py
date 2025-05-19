import random
import re
import shutil
import string
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from geh_common.tasks.TaskBase import TaskBase
from geh_common.telemetry import Logger, use_span

log = Logger(__name__)
DEFAULT_CSV_OPTIONS = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss'Z'"}
CHUNK_INDEX_COLUMN = "chunk_index_partition"


@dataclass
class FileInfo:
    source: Path
    destination: Path
    temporary: Path


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
    def create_zip_file(self, files_to_zip: list[FileInfo]) -> None:
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
                ref.write(fp.source, arcname=fp.destination)
        self.dbutils.fs.mv(f"file:{tmp_path}", self.zip_output_path)


def write_csv_files(
    df: DataFrame,
    output_path: str | Path,
    tmpdir: str | Path,
    file_name_factory: Callable[[str], str] = lambda x: x,
    partition_columns: list[str] | None = None,
    order_by: list[str] | None = None,
    rows_per_file: int | None = None,
    csv_options: dict[str, str] = DEFAULT_CSV_OPTIONS,
) -> list[str]:
    """Write a DataFrame to multiple files.

    Args:
        df (DataFrame): The DataFrame to write.
        output_path (str | Path): The path to write the files to.
        tmpdir (str | Path, optional): The temporary directory to write the files to. Defaults to "/tmp".
        partition_columns (list[str], optional): The columns to partition by. Defaults to [].
        order_by (list[str], optional): The columns to order by. Defaults to [].
        rows_per_file (int | None, optional): The number of rows per file. Defaults to None.
        csv_options (dict[str, str], optional): The options for the CSV writer. Defaults to DEFAULT_CSV_OPTIONS.

    Returns:
        list[str]: Headers for the csv file.
    """
    random_dir = "".join(random.choices(string.ascii_lowercase, k=10))
    result_output_path = Path(output_path)
    spark_output_path = result_output_path / random_dir
    tmpdir = Path(tmpdir)
    headers = _write_dataframe(
        df=df,
        spark_output_path=spark_output_path,
        partition_columns=partition_columns,
        order_by=order_by,
        rows_per_file=rows_per_file,
        csv_options=csv_options,
    )
    file_info = _get_file_info(
        result_output_path=result_output_path,
        spark_output_path=spark_output_path,
        tmpdir=tmpdir,
        file_name_factory=file_name_factory,
    )
    content = _merge_content(file_info=file_info, headers=headers)
    shutil.rmtree(spark_output_path)
    shutil.rmtree(tmpdir)
    return content


def _get_file_info(
    result_output_path: str | Path,
    spark_output_path: str | Path,
    tmpdir: str | Path,
    file_name_factory: Callable[[str], str] = lambda x: x,
) -> list[FileInfo]:
    file_info = []
    for i, f in enumerate(Path(spark_output_path).rglob("*.csv")):
        file_name = f"chunk_{i}.csv"
        if CHUNK_INDEX_COLUMN in str(f):
            regex = f"/{CHUNK_INDEX_COLUMN}=([0-9]+)/"
            chunk_index = re.search(regex, str(f)).group(1)
            file_name = f"chunk_{chunk_index}.csv"
        file_name = file_name_factory(file_name)
        file_info.append(
            FileInfo(
                source=f,
                destination=result_output_path / file_name,
                temporary=tmpdir / file_name,
            )
        )
    if len(file_info) == 0:
        raise ValueError(f"No files found in {spark_output_path}")
    return file_info


def _write_dataframe(
    df: DataFrame,
    spark_output_path: str | Path,
    partition_columns: list[str] | None = None,
    order_by: list[str] | None = None,
    rows_per_file: int | None = None,
    csv_options: dict[str, str] = DEFAULT_CSV_OPTIONS,
) -> list[str]:
    """Write a DataFrame to multiple files.

    Args:
        df (DataFrame): The DataFrame to write.
        path (str): The path to write the files to.
        partition_columns (list[str], optional): The columns to partition by. Defaults to [].
        order_by (list[str], optional): The columns to order by. Defaults to [].
        rows_per_file (int | None, optional): The number of rows per file. Defaults to None.
        csv_options (dict[str, str], optional): The options for the CSV writer. Defaults to DEFAULT_CSV_OPTIONS.

    Returns:
        list[str]: Headers for the csv file.
    """
    if partition_columns is None:
        partition_columns = []
    if order_by is None:
        order_by = []

    if rows_per_file is not None and rows_per_file > 0:
        if len(order_by) == 0:
            for f in df.schema.fields:
                if isinstance(f.dataType, T.TimestampType | T.DateType):
                    order_by.append(f.name)
        if len(order_by) == 0:
            order_by.extend([c for c in df.columns if c not in partition_columns])
        w = Window().partitionBy(partition_columns).orderBy(order_by)
        df = df.select("*", F.ceil((F.row_number().over(w)) / F.lit(rows_per_file)).alias(CHUNK_INDEX_COLUMN))
        partition_columns.append(CHUNK_INDEX_COLUMN)
        log.info(f"Writing {rows_per_file} rows per file")

    if len(order_by) > 0:
        df = df.orderBy(*order_by)

    if partition_columns:
        df.write.mode("overwrite").options(**csv_options).partitionBy(partition_columns).csv(str(spark_output_path))
    else:
        df.write.mode("overwrite").options(**csv_options).csv(str(spark_output_path))

    return [c for c in df.columns if c not in partition_columns]


def _merge_content(file_info: list[FileInfo], headers: list[str]) -> list[str]:
    for info in file_info:
        info.temporary.parent.mkdir(parents=True, exist_ok=True)
        with info.temporary.open("w+") as fh_temporary:
            log.info("Creating " + str(info.temporary))
            fh_temporary.write(",".join(headers) + "\n")
        with info.source.open("r") as fh_source:
            with info.temporary.open("a") as fh_temporary:
                fh_temporary.write(fh_source.read())

    destinations = {f.destination: [] for f in file_info}
    for info in file_info:
        destinations[info.destination].append(info.temporary)

    for dst, tmp_files in destinations.items():
        log.info(f"Creating {dst}")
        with dst.open("a") as fh_destination:
            for tmp_file in tmp_files:
                log.info(f"Appending {tmp_file} to {dst}")
                with tmp_file.open("r") as fh_temporary:
                    fh_destination.write(fh_temporary.read())

    return list(destinations.keys())
