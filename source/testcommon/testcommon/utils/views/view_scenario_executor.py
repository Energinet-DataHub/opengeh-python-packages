from pyspark.sql import SparkSession

from testcommon.utils.typecasting import cast_column_types
from testcommon.utils.views.dataframe_wrapper import DataframeWrapper
from testcommon.utils.csv_to_dataframe_parser import CsvToDataframeWrapperParser


class ViewScenarioExecutor:
    parser: CsvToDataframeWrapperParser

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.parser = CsvToDataframeWrapperParser(spark)

    def execute(self, scenario_folder_path: str) -> list[DataframeWrapper]:
        input_dataframes_wrappers = self.parser.parse_csv_files_concurrently(
            f"{scenario_folder_path}/when"
        )

        self.types = self.correct_dataframe_types(input_dataframes_wrappers)
        input_dataframes_wrappers = self.types
        self._write_to_tables(input_dataframes_wrappers)

        output_dataframe_wrappers = self.parser.parse_csv_files_concurrently(
            f"{scenario_folder_path}/then"
        )

        actual = self._read_from_views(output_dataframe_wrappers)
        return actual

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
