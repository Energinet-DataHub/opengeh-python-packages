from enum import Enum


class TimeResolution(Enum):
    QUARTER_HOURLY = "quarter_hourly"
    HOURLY = "hourly"
    MONTHLY = "monthly"
    OTHER = "other"  # not used in datahub 3.0. Only exists for data older than 01.01.2021
