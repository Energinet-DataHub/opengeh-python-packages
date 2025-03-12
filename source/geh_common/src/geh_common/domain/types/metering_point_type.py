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
    ANALYSIS = "analysis"
    NOT_USED = "not_used"
    SURPLUS_PRODUCTION_GROUP_6 = "surplus_production_group_6"
    NET_LOSS_CORRECTION = "net_loss_correction"
    OTHER_CONSUMPTION = "other_consumption"
    OTHER_PRODUCTION = "other_production"
    EXCHANGE_REACTIVE_ENERGY = "exchange_reactive_energy"
    COLLECTIVE_NET_PRODUCTION = "collective_net_production"
    COLLECTIVE_NET_CONSUMPTION = "collective_net_consumption"
