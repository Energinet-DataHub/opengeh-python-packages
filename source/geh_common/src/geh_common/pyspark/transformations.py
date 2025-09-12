from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def convert_utc_to_localtime(df: DataFrame, timestamp_column: str, time_zone: str) -> DataFrame:
    return (
        df.select(
            "*",
            F.from_utc_timestamp(F.col(timestamp_column), time_zone).alias(f"{timestamp_column}_tmp"),
        )
        .drop(timestamp_column)
        .withColumnRenamed(f"{timestamp_column}_tmp", timestamp_column)
    )


def convert_localtime_to_utc(df: DataFrame, timestamp_column: str, time_zone: str) -> DataFrame:
    return (
        df.select(
            "*",
            F.to_utc_timestamp(F.col(timestamp_column), time_zone).alias(f"{timestamp_column}_tmp"),
        )
        .drop(timestamp_column)
        .withColumnRenamed(f"{timestamp_column}_tmp", timestamp_column)
    )


def get_timestamp_columns(df: DataFrame) -> list[str]:
    """Get all timestamp and date columns from DataFrame."""
    return [field.name for field in df.schema.fields if isinstance(field.dataType, (T.TimestampType))]


def _convert_timezone(
    df: DataFrame,
    time_zone: str,
    conversion_func: Callable[[DataFrame, str, str], DataFrame],
) -> DataFrame:
    """Convert all timestamp/date columns between UTC and local timezone.

    Args:
        df: Input DataFrame
        time_zone: Target timezone (e.g. 'Europe/Copenhagen')
        to_utc: If True converts local->UTC, if False converts UTC->local
    """
    timestamp_cols = get_timestamp_columns(df)

    if not timestamp_cols:
        return df

    result_df = df

    for col in timestamp_cols:
        result_df = conversion_func(result_df, col, time_zone)

    return result_df


def convert_from_utc(df: DataFrame, time_zone: str) -> DataFrame:
    return _convert_timezone(df, time_zone, conversion_func=convert_utc_to_localtime)


def convert_to_utc(df: DataFrame, time_zone: str) -> DataFrame:
    return _convert_timezone(df, time_zone, conversion_func=convert_localtime_to_utc)


def begining_of_year(date: Column, years_to_add: int = 0) -> Column:
    if date is None:
        return None
    start_of_year = F.date_trunc("year", date).cast(T.TimestampType())
    if years_to_add > 0:
        return F.add_months(start_of_year, 12 * years_to_add).cast(T.TimestampType())
    return start_of_year


def days_in_year(col: Column) -> Column:
    return F.dayofyear(
        F.date_add(
            begining_of_year(col, years_to_add=1),
            -1,
        )
    )


def convert_datetime_to_midnight_in_timezone(datetime: datetime, time_zone: str) -> datetime:
    """Convert a datetime to the specified timezone from UTC to midnight in the specified timezeone.

    Args:
      execution_start_datetime: The datetime to convert
      time_zone: The target timezone string (e.g. 'Europe/Copenhagen')

    Returns:
      The same date at midnight in the specified timezone
    """
    return datetime.astimezone(ZoneInfo(time_zone)).replace(hour=0, minute=0, second=0, microsecond=0)
