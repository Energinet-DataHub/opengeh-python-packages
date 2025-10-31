import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from geh_common.domain.types.metering_point_type import MeteringPointType
from geh_common.domain.types.quantity_quality import QuantityQuality
from geh_common.infrastructure.current_measurements_repository import CurrentMeasurementsRepository
from geh_common.infrastructure.model.current_measurements import CURRENT_MEASUREMENTS_SCHEMA


@pytest.fixture(scope="module")
def current_measurements_repository(spark: SparkSession) -> CurrentMeasurementsRepository:
    return CurrentMeasurementsRepository(
        spark=spark,
        catalog_name=spark.catalog.currentCatalog(),
    )


@pytest.fixture(scope="module")
def valid_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(
        data=[
            (
                "123456789012345678",
                datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("Europe/Copenhagen")),
                Decimal("1.123"),
                QuantityQuality.MEASURED,
                MeteringPointType.CONSUMPTION,
                "678",
                2023,
                1,
            )
        ],
        schema=CURRENT_MEASUREMENTS_SCHEMA,
    )
    assert df.schema == CURRENT_MEASUREMENTS_SCHEMA
    return df


def test__when_invalid_contract__raises_with_useful_message(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    def mock_read_table(*args, **kwargs):
        return valid_df

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Act
    current_measurements = current_measurements_repository.read_current_measurements(
        period_start_utc=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("UTC")),
        period_end_utc=datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=ZoneInfo("UTC")),
    ).df
    # Assert
    current_measurements.show()


# lower bound
# upper bound
# metering point id
# skudår
# sommertid, vintertid

# tjek at filtreringen hverken fjerner for lidt og for meget

# TODO HENRIK: Lav test
