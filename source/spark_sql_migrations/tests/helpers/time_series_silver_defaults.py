from decimal import Decimal
from package.enums.time_series.resolution_enum import Dh3ResolutionEnum


class TimeSeriesSilverDefaults:
    default_type_of_mp: str = "E18"
    metering_point_id: str = "570500000000000041"
    grid_area_id: str = "244"
    historical_flag: str = "N"
    resolution: str = Dh3ResolutionEnum.PT1H.value
    unit: str = "KWH"
    number_of_values: int = 24
    quantity_start: Decimal = Decimal(1)
    quality: str = "E01"
