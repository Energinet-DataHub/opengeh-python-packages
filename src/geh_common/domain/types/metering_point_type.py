from enum import Enum


class MeteringPointType(Enum):
    CAPACITY_SETTLEMENT = "capacity_settlement"
    CONSUMPTION = "consumption"  # Parent
    CONSUMPTION_FROM_GRID = "consumption_from_grid"
    CONSUMPTION_METERING_POINT_TYPE = "consumption"
    ELECTRICAL_HEATING = "electrical_heating"
    EXCHANGE = "exchange"  # Parent
    NET_CONSUMPTION = "net_consumption"
    NET_FROM_GRID = "net_from_grid"
    NET_PRODUCTION = "net_production"
    NET_TO_GRID = "net_to_grid"
    OWN_PRODUCTION = "own_production"
    PRODUCTION = "production"  # Parent
    SUPPLY_TO_GRID = "supply_to_grid"
    TOTAL_CONSUMPTION = "total_consumption"
    VE_PRODUCTION = "ve_production"
    WHOLESALE_SERVICES_INFORMATION = "wholesale_services_information"
