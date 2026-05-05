from enum import Enum


class AssetType(Enum):
    UNKNOWN = "unknown"
    STEAM_TURBINE_WITH_BACK_PRESSURE_MODE = "steam_turbine_with_back_pressure_mode"
    GAS_TURBINE = "gas_turbine"
    COMBINED_CYCLE = "combined_cycle"
    COMBUSTION_ENGINE_GAS = "combustion_engine_gas"
    STEAM_TURBINE_WITH_CONDENSATION_STEAM = "steam_turbine_with_condensation_steam"
    BOILER = "boiler"
    STIRLING_ENGINE = "stirling_engine"
    PERMANENT_CONNECTED_ELECTRICAL_ENERGY_STORAGE_FACILITIES = (
        "permanent_connected_electrical_energy_storage_facilities"
    )
    TEMPORARILY_CONNECTED_ELECTRICAL_ENERGY_STORAGE_FACILITIES = (
        "temporarily_connected_electrical_energy_storage_facilities"
    )
    FUEL_CELLS = "fuel_cells"
    PHOTO_VOLTAIC_CELLS = "photo_voltaic_cells"
    WIND_TURBINES = "wind_turbines"
    HYDROELECTRIC_POWER = "hydroelectric_power"
    WAVE_POWER = "wave_power"
    MIXED_PRODUCTION = "mixed_production"
    PRODUCTION_WITH_ELECTRICAL_ENERGY_STORAGE_FACILITIES = "production_with_electrical_energy_storage_facilities"
    POWER_TO_X = "power_to_x"
    REGENERATIVE_DEMAND_FACILITY = "regenerative_demand_facility"
    COMBUSTION_ENGINE_DIESEL = "combustion_engine_diesel"
    COMBUSTION_ENGINE_BIO = "combustion_engine_bio"
    NO_TECHNOLOGY = "no_technology"
    UNKNOWN_TECHNOLOGY = "unknown_technology"
