from enum import Enum


class AggregatedResolution(Enum):
    """Enum for aggregated resolution types."""

    SUM_OF_DAY = "sum_of_day"
    SUM_OF_MONTH = "sum_of_month"
    ACTUAL_RESOLUTION = "actual_resolution"
