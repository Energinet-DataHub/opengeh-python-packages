from typing import Tuple

from pyspark.sql import SparkSession

from source.testcommon.testcommon.utils.csv_to_dataframe_parser import CsvToDataframeWrapperParser
from source.testcommon.testcommon.utils.dataframes.typecasting import cast_column_types
from source.testcommon.testcommon.utils.views.dataframe_wrapper import DataframeWrapper


class ViewScenarioExecutor:
    parser: CsvToDataframeWrapperParser

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.parser = CsvToDataframeWrapperParser(spark)

    def execute(
            self, scenario_folder_path: str
    ) -> Tuple[list[DataframeWrapper], list[DataframeWrapper]]:

        input_dataframes_wrappers = self.parser.parse_csv_files_concurrently(
            f"{scenario_folder_path}/when"
        )

        input_dataframes_wrappers = self.correct_dataframe_types(
            input_dataframes_wrappers
        )
        self._write_to_tables(input_dataframes_wrappers)

        output_dataframe_wrappers = self.parser.parse_csv_files_concurrently(
            f"{scenario_folder_path}/then"
        )

        expected = self.correct_dataframe_types(output_dataframe_wrappers)

        actual = self._read_from_views(output_dataframe_wrappers)
        return actual, expected

    @staticmethod
    def _write_to_tables(
            input_dataframe_wrappers: list[DataframeWrapper],
    ) -> None:
        for wrapper in input_dataframe_wrappers:
            try:
                wrapper.df.write.format("delta").mode("overwrite").saveAsTable(
                    wrapper.name
                )
            except Exception as e:
                raise Exception(f"Failed to write to table {wrapper.name}") from e

    def _read_from_views(
            self,
            output_dataframe_wrappers: list[DataframeWrapper],
    ) -> list[DataframeWrapper]:

        wrappers = []
        for wrapper in output_dataframe_wrappers:
            df = self.spark.read.format("delta").table(wrapper.name)
            dataframe_wrapper = DataframeWrapper(
                key=wrapper.key, name=wrapper.name, df=df
            )
            wrappers.append(dataframe_wrapper)

        return wrappers

    def correct_dataframe_types(
            self,
            dataframe_wrappers: list[DataframeWrapper],
    ) -> list[DataframeWrapper]:
        wrappers = []
        for wrapper in dataframe_wrappers:
            if wrapper.df is None:
                continue
            wrapper.df = cast_column_types(wrapper.df, table_or_view_name=wrapper.name)
            wrappers.append(wrapper)

        return wrappers
