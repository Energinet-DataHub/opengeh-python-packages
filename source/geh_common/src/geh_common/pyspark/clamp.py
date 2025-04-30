from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import Column


def clamp_period_start(col: str | Column, clamp_start_datetime: datetime) -> Column:
    """Clamp the start of a period to a given datetime."""
    if isinstance(col, str):
        col = F.col(col)
    return F.when(col.isNull() | (col < clamp_start_datetime), clamp_start_datetime).otherwise(col)


def clamp_period_end(col: str | Column, clamp_end_datetime: datetime) -> Column:
    """Clamp the end of a period to a given datetime."""
    if isinstance(col, str):
        col = F.col(col)
    return F.when(col.isNull() | (col > clamp_end_datetime), clamp_end_datetime).otherwise(col)
