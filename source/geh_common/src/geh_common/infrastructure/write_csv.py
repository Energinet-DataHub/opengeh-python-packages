import random
import string
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from geh_common.telemetry import Logger

log = Logger(__name__)
DEFAULT_CSV_OPTIONS = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss'Z'"}
CHUNK_INDEX_COLUMN = "chunk_index_partition"

FileNameCallbackType = Callable[[str, dict[str, str]], str]


def _default_file_name_callback(file_name: str, partitions: dict[str, str]) -> str:
    """Create default file name factory function.

    Args:
        file_name (str): The original file name.
        partitions (dict[str, str]): The partitions included in the original file path.

    Returns:
        str: The new file name.
    """
    return file_name


@dataclass
class FileInfo:
    """Class to hold file information.

    Attributes:
        source (Path): The source file path.
        destination (Path): The destination file path.
        temporary (Path): The temporary file path.
    """

    source: Path
    destination: Path
    temporary: Path


def write_csv_files(
    df: DataFrame,
    output_path: str | Path,
    file_name_factory: FileNameCallbackType = _default_file_name_callback,
    spark_output_path: str | Path | None = None,
    tmpdir: str | Path | None = None,
    partition_columns: list[str] | None = None,
    order_by: list[str] | None = None,
    rows_per_file: int | None = None,
    csv_options: dict[str, str] = DEFAULT_CSV_OPTIONS,
) -> list[Path]:
    """Write a DataFrame to multiple files.

    Args:
        df (DataFrame): The DataFrame to write.
        output_path (str | Path): The path to write the files to.
        file_name_factory (FileNameCallbackType, optional): The function to create the file name. Defaults to DefaultFileNameCallback.
        spark_output_path (str | Path, optional): The path to the Spark output directory. Defaults to None.
        tmpdir (str | Path | None, optional): The temporary directory to write the files to. Defaults to None.
        partition_columns (list[str], optional): The columns to partition by. Defaults to [].
        order_by (list[str], optional): The columns to order by. Defaults to [].
        rows_per_file (int | None, optional): The number of rows per file. Defaults to None.
        csv_options (dict[str, str], optional): The options for the CSV writer. Defaults to DEFAULT_CSV_OPTIONS.

    Returns:
        list[Path]: The list of file paths created.
    """
    random_dir = "".join(random.choices(string.ascii_lowercase, k=10))
    result_output_path = Path(output_path)
    if spark_output_path is None:
        spark_output_path = result_output_path / random_dir
    tmpdir = tmpdir or Path("/tmp") / random_dir
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
    files = _merge_content(file_info=file_info, headers=headers)
    return files


def get_partition_information(path) -> dict[str, str]:
    """Extract partition information from a file path.

    Args:
        path (str or Path): The file path from which to extract partition information.
            The path should contain partition information in the format "key=value".
            For example, "/tmp/part=1/part2=2/test" would yield {"part": "1", "part2": "2"}.

    Raises:
        ValueError: If the path contains an invalid partition format (e.g., "key=value=value").
            This error occurs when there are too many values to unpack from the partition string.

    Returns:
        dict[str, str]: A dictionary with partition names as keys and their values.
    """
    partitions = {}
    for p in Path(path).parts:
        if "=" in p:
            key, value = p.split("=", 2)
            partitions[key] = value
    return partitions


def _get_file_info(
    result_output_path: str | Path,
    spark_output_path: str | Path,
    tmpdir: str | Path,
    file_name_factory: FileNameCallbackType,
) -> list[FileInfo]:
    """Get file information for the files to be zipped.

    Args:
        result_output_path (str | Path): The path to the output directory.
        spark_output_path (str | Path): The path to the Spark output directory.
        tmpdir (str | Path): The temporary directory to write the files to.
        file_name_factory (FileFactoryType, optional): The function to create the file name. Defaults to FileFactoryDefault.

    Raises:
        ValueError: If no files are found in the spark output path.

    Returns:
        list[FileInfo]: The list of file information.
    """
    file_info = []
    for i, f in enumerate(Path(spark_output_path).rglob("*.csv")):
        file_name = f"chunk_{i}.csv"
        partitions = get_partition_information(f)
        file_name = file_name_factory(file_name, partitions)
        file_info.append(
            FileInfo(
                source=f,
                destination=Path(result_output_path) / file_name,
                temporary=Path(tmpdir) / file_name,
            )
        )
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
        spark_output_path (str | Path): The path to write the files to.
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
    partition_columns = partition_columns.copy()
    order_by = order_by.copy()
    csv_options = csv_options.copy()

    if rows_per_file is not None and rows_per_file > 0:
        if len(order_by) == 0:
            for f in df.schema.fields:
                if isinstance(f.dataType, T.TimestampType | T.DateType):
                    order_by.append(f.name)
        if len(order_by) == 0:
            order_by.append(df.columns[0])
        w = Window().partitionBy(partition_columns).orderBy(order_by)
        df = df.select("*", F.ceil((F.row_number().over(w)) / F.lit(rows_per_file)).alias(CHUNK_INDEX_COLUMN))
        unique_chunk_index = df.select(CHUNK_INDEX_COLUMN).distinct().count()
        if unique_chunk_index > 1:
            partition_columns.append(CHUNK_INDEX_COLUMN)
        else:
            df = df.drop(CHUNK_INDEX_COLUMN)
        log.info(f"Writing {rows_per_file} rows per file")

    if len(order_by) > 0:
        df = df.orderBy(*order_by)

    if partition_columns:
        df.write.mode("overwrite").options(**csv_options).partitionBy(partition_columns).csv(str(spark_output_path))
    else:
        df.write.mode("overwrite").options(**csv_options).csv(str(spark_output_path))

    return [c for c in df.columns if c not in partition_columns]


def _merge_content(file_info: list[FileInfo], headers: list[str]) -> list[Path]:
    """Merge the content of the files into a single file.

    Args:
        file_info (list[FileInfo]): The list of file information.
        headers (list[str]): The headers for the CSV file.

    Returns:
        list[Path]: The headers for the CSV file.
    """
    tmp_destinations = {info.temporary: set() for info in file_info}
    for info in file_info:
        tmp_destinations[info.temporary].add(info.source)

    for tmp, sources in tmp_destinations.items():
        log.info(f"Creating {tmp}")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w+") as fh_temporary:
            fh_temporary.write(",".join(headers) + "\n")
            for source in sources:
                log.info(f"Appending {source} to {tmp}")
                with source.open("r") as fh_source:
                    fh_temporary.write(fh_source.read())

    destinations = {info.destination: set() for info in file_info}
    for info in file_info:
        destinations[info.destination].add(info.temporary)

    for dst, tmp_files in destinations.items():
        log.info(f"Creating {dst}")
        with dst.open("a") as fh_destination:
            for tmp_file in tmp_files:
                log.info(f"Appending {tmp_file} to {dst}")
                with tmp_file.open("r") as fh_temporary:
                    fh_destination.write(fh_temporary.read())

    return list(destinations.keys())
