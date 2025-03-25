from enum import Enum


class MeteringPointType(Enum):
    ANALYSIS = "analysis"
    CAPACITY_SETTLEMENT = "capacity_settlement"
    COLLECTIVE_NET_CONSUMPTION = "collective_net_consumption"
    COLLECTIVE_NET_PRODUCTION = "collective_net_production"
    CONSUMPTION = "consumption"  # Parent
    CONSUMPTION_FROM_GRID = "consumption_from_grid"
    CONSUMPTION_METERING_POINT_TYPE = "consumption"
    ELECTRICAL_HEATING = "electrical_heating"
    EXCHANGE = "exchange"  # Parent
    EXCHANGE_REACTIVE_ENERGY = "exchange_reactive_energy"
    INTERNAL_USE = "internal_use"
    NET_CONSUMPTION = "net_consumption"
    NET_FROM_GRID = "net_from_grid"
    NET_LOSS_CORRECTION = "net_loss_correction"
    NET_PRODUCTION = "net_production"
    NET_TO_GRID = "net_to_grid"
    NOT_USED = "not_used"
    OTHER_CONSUMPTION = "other_consumption"
    OTHER_PRODUCTION = "other_production"
    OWN_PRODUCTION = "own_production"
    PRODUCTION = "production"  # Parent
    SUPPLY_TO_GRID = "supply_to_grid"
    SURPLUS_PRODUCTION_GROUP_6 = "surplus_production_group_6"
    TOTAL_CONSUMPTION = "total_consumption"
    VE_PRODUCTION = "ve_production"
    WHOLESALE_SERVICES_INFORMATION = "wholesale_services_information"
