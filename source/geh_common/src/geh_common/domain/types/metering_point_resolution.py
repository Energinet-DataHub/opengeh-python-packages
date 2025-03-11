from enum import Enum


class MeteringPointResolution(Enum):
    FIFTEEN_MINUTES = "PT15M"
    HOUR = "PT1H"
    QUARTER = "PT15M"
