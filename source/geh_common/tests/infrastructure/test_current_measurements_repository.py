from datetime import UTC, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from geh_common.data_products.measurements_core.measurements_gold import measurements_zorder
from geh_common.domain.types import MeteringPointType, QuantityQuality
from geh_common.infrastructure.current_measurements_repository import CurrentMeasurementsRepository
from geh_common.infrastructure.model.current_measurements import CURRENT_MEASUREMENTS_SCHEMA


def make_measurement_row(
    metering_point_id: str,
    observation_time: datetime,
) -> tuple:
    """Create a measurement row where only metering_point_id and observation_time is needed and used."""
    return (
        metering_point_id,  # METERING_POINT_ID
        "",  # NOT USED
        "",  # NOT USED
        observation_time,  # OBSERVATION_TIME
        Decimal("1.123"),  # NOT USED
        QuantityQuality.MEASURED.value,  # NOT USED
        MeteringPointType.CONSUMPTION.value,  # NOT USED
        "",  # NOT USED
        "",  # NOT USED
        "",  # NOT USED
        observation_time,  # NOT USED
        observation_time,  # NOT USED
        observation_time,  # NOT USED
        1,  # NOT USED
        1,  # NOT USED
        1,  # NOT USED
    )


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
            make_measurement_row(
                "123456789012345678",
                datetime(2023, 1, 1, 0, 0, 0),
            )
        ],
        schema=measurements_zorder.schema,
    )
    assert df.schema == measurements_zorder.schema
    return df


def test__when_valid_contract__return_required_columns(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    period_start_utc = datetime(2022, 1, 1)
    period_end_utc = datetime(2023, 1, 31)

    def mock_read_table(*args, **kwargs) -> DataFrame:
        return valid_df

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Act
    actual = current_measurements_repository.read_current_measurements(
        period_start_utc=period_start_utc, period_end_utc=period_end_utc
    )

    # Assert
    assert actual.df.schema == CURRENT_MEASUREMENTS_SCHEMA


def test__when_invalid_contract__raises_with_useful_message(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    invalid_df = valid_df.drop(F.col("quantity"))
    period_start_utc = datetime(2022, 1, 1)
    period_end_utc = datetime(2023, 1, 31)

    def mock_read_table(*args, **kwargs) -> DataFrame:
        return invalid_df

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Assert
    with pytest.raises(
        Exception,
        match=r"The data source does not comply with the contract.*",
    ):
        # Act
        current_measurements_repository.read_current_measurements(
            period_start_utc=period_start_utc, period_end_utc=period_end_utc
        )


def test__when_source_contains_unexpected_columns__returns_data_without_unexpected_column(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the repository can handle columns being added as it is defined to NOT be a breaking change.
    The repository should return the data without the unexpected column."""
    # Arrange
    period_start_utc = datetime(2022, 1, 1)
    period_end_utc = datetime(2023, 1, 31)

    valid_df_with_extra_col = valid_df.withColumn("extra_col", F.lit("extra_value"))

    def mock_read_table(*args, **kwargs) -> DataFrame:
        return valid_df_with_extra_col

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Act
    actual = current_measurements_repository.read_current_measurements(
        period_end_utc=period_end_utc, period_start_utc=period_start_utc
    )

    # Assert
    assert actual.df.schema == CURRENT_MEASUREMENTS_SCHEMA


def test__data_is_contained_within_the_period(
    spark: SparkSession,
    current_measurements_repository: CurrentMeasurementsRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    period_end_utc = datetime(2024, 12, 31, 23, 0, 0, tzinfo=UTC)

    period_start_local_time = datetime(2021, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("Europe/Copenhagen"))
    period_start_utc = period_start_local_time.astimezone(UTC)

    def mock_read_table(*args, **kwargs) -> DataFrame:
        return spark.createDataFrame(
            data=[
                make_measurement_row("100000000000000001", period_start_utc - timedelta(seconds=1)),
                make_measurement_row("100000000000000002", period_start_utc),
                make_measurement_row("100000000000000003", period_start_utc + timedelta(seconds=1)),
                make_measurement_row("100000000000000004", period_end_utc - timedelta(seconds=1)),
                make_measurement_row("100000000000000005", period_end_utc),
                make_measurement_row("100000000000000006", period_end_utc + timedelta(seconds=1)),
            ],
            schema=measurements_zorder.schema,
        )

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Act
    actual = current_measurements_repository.read_current_measurements(
        period_start_utc=period_start_utc,
        period_end_utc=period_end_utc,
    )

    # Assert
    actual_ids = [row.metering_point_id for row in actual.df.collect()]
    assert actual_ids == ["100000000000000002", "100000000000000003", "100000000000000004"]


def test__providing_list_of_metering_point_ids_limits_read(
    spark: SparkSession,
    current_measurements_repository: CurrentMeasurementsRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    metering_point_ids = [
        "100000000000000001",
    ]
    period_start_utc = datetime(2022, 1, 1, tzinfo=UTC)
    period_end_utc = datetime(2023, 1, 31, tzinfo=UTC)

    def mock_read_table(*args, **kwargs) -> DataFrame:
        return spark.createDataFrame(
            data=[
                make_measurement_row(metering_point_ids[0], period_start_utc),  # Inside: Metering point id included
                make_measurement_row("100000000000000002", period_start_utc),  # Outside: Metering point id not included
            ],
            schema=measurements_zorder.schema,
        )

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Act
    actual = current_measurements_repository.read_current_measurements(
        period_start_utc=period_start_utc,
        period_end_utc=period_end_utc,
        metering_point_ids=metering_point_ids,
    )

    # Assert
    actual_ids = [row.metering_point_id for row in actual.df.collect()]
    assert actual_ids == metering_point_ids


def test__period_filtering_during_dst_change(
    spark: SparkSession,
    current_measurements_repository: CurrentMeasurementsRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    period_start_utc = datetime(2024, 3, 31, 0, 0, tzinfo=UTC)  # day of DST change in Europe/Copenhagen
    period_end_utc = datetime(2024, 3, 31, 4, 0, tzinfo=UTC)

    def mock_read_table(*args, **kwargs) -> DataFrame:
        return spark.createDataFrame(
            data=[
                # Just before DST change
                make_measurement_row(
                    "100000000000000001",
                    datetime(2024, 3, 31, 0, 59, tzinfo=UTC),  # 01:59 Europe/Copenhagen
                ),
                # Right after DST change
                make_measurement_row(
                    "100000000000000002",
                    datetime(2024, 3, 31, 1, 0, tzinfo=UTC),  # 03:00 Europe/Copenhagen
                ),
                # Outside range
                make_measurement_row(
                    "100000000000000003",
                    datetime(2024, 3, 31, 4, 1, tzinfo=UTC),  # 06:01 Europe/Copenhagen
                ),
            ],
            schema=measurements_zorder.schema,
        )

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Act
    actual = current_measurements_repository.read_current_measurements(
        period_start_utc=period_start_utc, period_end_utc=period_end_utc
    )

    # Assert
    actual_records = [(row.metering_point_id, row.observation_time) for row in actual.df.collect()]
    expected_records = [
        ("100000000000000001", datetime(2024, 3, 31, 0, 59)),
        ("100000000000000002", datetime(2024, 3, 31, 1, 0)),
    ]
    assert actual_records == expected_records
