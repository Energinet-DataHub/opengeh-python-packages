from enum import Enum


class Product(Enum):
    TARIFF = "tariff"
    FUEL_QUANTITY = "fuel_quantity"
    POWER_ACTIVE = "power_active"
    POWER_REACTIVE = "power_reactive"
    ENERGY_ACTIVE = "energy_active"
    ENERGY_REACTIVE = "energy_reactive"
