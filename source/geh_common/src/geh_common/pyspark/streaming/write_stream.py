from typing import Callable

from pyspark.sql import DataFrame

from geh_common.pyspark.streaming.streaming_settings import StreamingSettings


def stream(
    dataframe: DataFrame,
    checkpoint_location: str,
    batch_operation: Callable[["DataFrame", int], None],
    output_mode: str = "append",
    format: str = "delta",
    write_stream = (
        dataframe.writeStream.outputMode(output_mode).format(format).option("checkpointLocation", checkpoint_location)
    )

    write_stream = StreamingSettings().apply_streaming_settings(write_stream)

    return write_stream.foreachBatch(batch_operation).start().awaitTermination()
