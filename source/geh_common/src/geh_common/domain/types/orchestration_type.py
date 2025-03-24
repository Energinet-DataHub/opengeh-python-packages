from enum import Enum


class OrchestrationType(Enum):
    ELECTRICAL_HEATING = "electrical_heating"
    """Process for calculation of electrical heating for households with electrical heating."""
    CAPACITY_SETTLEMENT = "capacity_settlement"
    """Process for calculating the capacity settlement for the largest consumers on the grid."""
    SUBMITTED = "submitted"
    """Process for submitting measurements from the process manager to measurements core."""
    MIGRATION = "migration"
    """Process for transferring measurements from subsystem 'datamigrations' to measurements core.
    The measurements from 'datamigrations' are migrated from DataHub 2."""
    NET_CONSUMPTION = "net_consumption"
    """Process that calculates the net consumption for net settlement group 6 households."""
    MISSING_MEASUREMENTS_LOG = "missing_measurements_log"
    """Process for calculating the missing metering points measurements in all grid areas."""
    MISSING_MEASUREMENTS_LOG_ON_DEMAND = "missing_measurements_log_on_demand"
    """Process for calculating the missing metering points measurements in selected grid areas."""
