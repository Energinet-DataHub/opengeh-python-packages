import pyspark.sql.types as t
from pyspark.sql import DataFrame

from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper

nullable = True


class CurrentMeasurements(DataFrameWrapper):
    """Current (latest) measurements from measurements_gold."""

    schema = t.StructType(
        [
            t.StructField("metering_point_id", t.StringType(), not nullable),
            t.StructField("observation_time", t.TimestampType(), not nullable),
            t.StructField("quantity", t.DecimalType(18, 3), nullable),
            t.StructField("quality", t.StringType(), not nullable),
            t.StructField("metering_point_type", t.StringType(), not nullable),
        ]
    )

    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=self.schema,
            ignore_nullability=True,
        )
