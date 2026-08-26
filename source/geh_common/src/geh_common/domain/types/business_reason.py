from enum import Enum


class BusinessReason(Enum):
    END_OF_SUPPLY = "end_of_supply"
    UPDATE_CHARGE_LINKS = "update_charge_links"
    DATA_ALIGNMENT_FOR_MASTER_DATA_METERING_POINT = "data_alignment_for_master_data_metering_point"
    UPDATE_CHARGE_INFORMATION = "update_charge_information"
    CUSTOMER_MOVE_IN = "customer_move_in"
    SECONDARY_MOVE_IN = "secondary_move_in"
    CUSTOMER_MOVE_OUT = "customer_move_out"
    UPDATE_MASTER_DATA_CONSUMER = "update_master_data_consumer"
    NEW_METERING_POINT = "new_metering_point"
    MOVE = "move"
    CONNECT_METERING_POINT = "connect_metering_point"
    CHANGE_CONNECTION_STATUS = "change_connection_status"
    CLOSE_DOWN_METERING_POINT = "close_down_metering_point"
    ELECTRICAL_HEATING = "electrical_heating"
    CHANGE_OF_ENERGY_SUPPLIER = "change_of_energy_supplier"
    PROCESS_CANCELLED_BY_REQUESTING_PARTY = "process_cancelled_by_requesting_party"
    PRODUCTION_OBLIGATION = "production_obligation"
    NO_DISCONNECTION_OF_METERING_POINT = "no_disconnection_of_metering_point"
    SERVICE_REQUEST = "service_request"
    CANCEL_SERVICE_REQUEST = "cancel_service_request"
    PERIODIC_METERING = "periodic_metering"
    PERIODIC_FLEX_METERING = "periodic_flex_metering"
    CANCELLATION = "cancellation"
    CHANGE_OF_SUPPLY_TO_SUPPLIER_OF_LAST_RESORT = "change_of_supply_to_supplier_of_last_resort"
    CONTINUE_SUPPLY_DUE_TO_REJECTED_REALLOCATE = "continue_supply_due_to_rejected_reallocate"
    DATE_OF_SUPPLIER_CHANGE_CAUSED_BY_END_OF_SUPPLY = "date_of_supplier_change_caused_by_end_of_supply"
    END_SUPPLY_DUE_TO_REALLOCATE = "end_supply_due_to_reallocate"
    HISTORICAL_INFORMATION_ABOUT_CONSUMPTION = "historical_information_about_consumption"
    INCORRECT_MOVE = "incorrect_move"
    INCORRECT_PROCESS = "incorrect_process"
    MERGE_OF_GRIDS = "merge_of_grids"
    MISSING_FLEX_TIME_SERIES = "missing_flex_time_series"
    MISSING_MEASUREMENTS_LOG = "missing_measurements_log"
    MISSING_NON_PROFILED_TIME_SERIES = "missing_non_profiled_time_series"
    MISSING_PROFILED_READING = "missing_profiled_reading"
    PREPARATION_FOR_IMBALANCE_SETTLEMENT = "preparation_for_imbalance_settlement"
    PROCESS_CANCELLED_BY_ITX = "process_cancelled_by_itx"
    PROPOSAL_CONTACT_INFORMATION = "proposal_contact_information"
    REMINDER = "reminder"
    REMOVED_PARENT_RELATION_ON_METERING_POINT = "removed_parent_relation_on_metering_point"
    ROLLBACK_CHANGE_OF_SUPPLIER = "rollback_change_of_supplier"
    TRANSFER_METERING_POINT = "transfer_metering_point"
    UNREQUESTED_CHANGE_OF_ENERGY_SUPPLIER = "unrequested_change_of_energy_supplier"
    UPDATE_MASTER_DATA_METER = "update_master_data_meter"
    CHANGE_OF_ESTIMATED_ANNUAL_VOLUME = "change_of_estimated_annual_volume"
    BALANCE_FIXING = "balance_fixing"  # obsolete: use BALANCE_SETTLEMENT
    BALANCE_SETTLEMENT = "balance_settlement"
    PRELIMINARY_AGGREGATION = "preliminary_aggregation"  # obsolete: use TEMPORARY
    TEMPORARY = "temporary"
    WHOLESALE_FIXING = "wholesale_fixing"  # obsolete: use WHOLE_SETTLEMENT
    WHOLE_SETTLEMENT = "whole_settlement"
    CORRECTION = "correction"  # obsolete: use CORRECTION_SETTLEMENT
    CORRECTION_SETTLEMENT = "correction_settlement"
    YEARLY_METERING = "yearly_metering"  # obsolete: use HISTORICAL_DATA
    HISTORICAL_DATA = "historical_data"
    UPDATE_CHARGE_SERIES = "update_charge_series"  # obsolete: use UPDATE_CHARGE_PRICES
    UPDATE_CHARGE_PRICES = "update_charge_prices"
    UPDATE_METERING_POINT_MASTER_DATA = (
        "update_metering_point_master_data"  # obsolete: use UPDATE_MASTER_DATA_METERING_POINT
    )
    UPDATE_MASTER_DATA_METERING_POINT = "update_master_data_metering_point"
    REQUEST_CHARGE_PRICES = "request_charge_prices"  # obsolete: use REQUEST_FOR_CHARGE_PRICES
    REQUEST_FOR_CHARGE_PRICES = "request_for_charge_prices"
