import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame

from geh_common.testing.dataframes.assert_schemas import assert_schema


class DataFrameWrapper:
    """Base class of "typed" data frames.

    The raw pyspark DataFrame is accessible as `data.df`.

    The wrapper will transform and verify the provided data frame:
    - Missing nullable columns will be added
    - Columns not present in the schema is removed
    - The resulting data frame is validated against the schema regarding types and
      nullability
    """

    def __init__(
        self,
        df: DataFrame,
        schema: t.StructType,
        ignore_nullability: bool = False,
        ignore_decimal_scale: bool = False,
        ignore_decimal_precision: bool = False,
    ):
        df = DataFrameWrapper._add_missing_nullable_columns(df, schema)

        columns = [field.name for field in schema.fields]
        df = df.select(columns)

        assert_schema(
            df.schema,
            schema,
            ignore_nullability=ignore_nullability,
            ignore_column_order=True,
            ignore_decimal_scale=ignore_decimal_scale,
            ignore_decimal_precision=ignore_decimal_precision,
        )

        self._df: DataFrame = df

    @property
    def df(self) -> DataFrame:
        return self._df

    @staticmethod
    def _add_missing_nullable_columns(df: DataFrame, schema: t.StructType) -> DataFrame:
        """Add missing nullable columns to the data frame.

        Utility method to add nullable fields that are expected by the schema,
        but are not present in the actual data frame.

        Args:
            df: The data frame to add nullable columns to.
            schema: The schema to compare against.

        Returns:
            The data frame with missing nullable columns added.
        """
        for expected_field in schema:
            if expected_field.nullable and all(
                actual_field.name != expected_field.name for actual_field in df.schema.fields
            ):
                df = df.withColumn(
                    expected_field.name,
                    f.lit(None).cast(expected_field.dataType),
                )

        return df

    def cache_internal(self) -> None:
        self._df = self._df.cache()
