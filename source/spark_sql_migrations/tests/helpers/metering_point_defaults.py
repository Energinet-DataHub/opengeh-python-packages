from package.enums.metering_points.dossier_status_enum import DossierStatusEnum
from package.enums.metering_points.type_of_mp_enum import TypeOfMpEnum
from package.enums.metering_points.accepted_enum import AcceptedEnum
from package.enums.metering_points.current_step_status_enum import CurrentStepStatusEnum
from package.enums.metering_points.step_type_enum import StepTypeEnum
from package.enums.metering_points.physical_status_of_mp_enum import (
    PhysicalStatusOfMpEnum,
)
from package.enums.metering_points.wholesale_settlement_method_enum import (
    SettlementMethodEnum,
)
from package.enums.metering_points.wholesale_meter_reading_occurrence_enum import (
    MeterReadingOccurrenceEnum,
)
import tests.helpers.datetime_formatter as datetime_formatter


class MeteringPointDefaults:
    default_dossier_status: str = DossierStatusEnum.CAN.value
    rescued_data: str | None = None
    metering_point_id: str = "570500000000000041"
    btd_business_trans_doss_id: int = 84356
    metering_point_state_id: int = 35075
    metering_grid_area_id: str = "500"
    type_of_mp: str = TypeOfMpEnum.E17.value
    accepted: str = AcceptedEnum.A1.value
    bus_trans_doss_id: int = 115593
    transaction_type: str = "VIEWMPNO"
    bus_trans_doss_step_id: int = 430621
    current_step_status: str = CurrentStepStatusEnum.REC.value
    step_type: str = StepTypeEnum.VIEWMPOREQ.value
    physical_status_of_mp: str = PhysicalStatusOfMpEnum.E22.value
    settlement_method: str = SettlementMethodEnum.E02.value
    meter_reading_occurrence: str = MeterReadingOccurrenceEnum.PT1H.value
    balance_supplier_id: str = "57X0000000002993"
    balance_responsible_party_id: str = "5705000001"
    balance_supplier_start_date = datetime_formatter.convert_to_datetime("2023-01-02T23:00:00.000")
    balance_resp_party_start_date = datetime_formatter.convert_to_datetime(
        "2023-01-02T23:00:00.000")
    from_grid_area = None
    to_grid_area = None
