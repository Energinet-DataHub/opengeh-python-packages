from enum import Enum


class MeteringPointResolutionLegacy(Enum):
    """Legacy metering point resolution types. Use TimeResolution instead."""

    HOUR = "PT1H"
    QUARTER = "PT15M"
    MONTH = "P1M"
