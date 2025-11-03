from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from geh_common.domain.types import MeteringPointType, QuantityQuality
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
                datetime(2023, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("Europe/Copenhagen")),
                Decimal("1.123"),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
                678,
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
    invalid_df = valid_df.drop(F.col("quantity"))
    period_start_utc = datetime(2022, 1, 1)
    period_end_utc = datetime(2023, 1, 31)

    def mock_read_table(*args, **kwargs):
        return invalid_df

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Assert
    with pytest.raises(
        Exception,
        match=r"UNRESOLVED_COLUMN.WITH_SUGGESTION",
    ):
        # Act
        current_measurements = current_measurements_repository.read_current_measurements(
            period_start_utc=period_start_utc, period_end_utc=period_end_utc
        )
        current_measurements.df.show()


def test__when_source_contains_unexpected_columns__returns_data_without_unexpected_column(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the repository can handle columns being added as it is defined to _not_ be a breaking change.
    The repository should return the data without the unexpected column."""
    # Arrange
    period_start_utc = datetime(2022, 1, 1)
    period_end_utc = datetime(2023, 1, 31)

    valid_df_with_extra_col = valid_df.withColumn("extra_col", F.lit("extra_value"))

    def mock_read_table(*args, **kwargs):
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
    execution_start_datetime_utc = datetime(
        year=2024, month=12, day=31, hour=23, minute=0, second=0, tzinfo=ZoneInfo("UTC")
    )
    execution_start_datetime_local_time = execution_start_datetime_utc.astimezone(ZoneInfo("Europe/Copenhagen"))
    execution_start_date_local_time_midnight = execution_start_datetime_local_time.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    period_end_local_time = execution_start_date_local_time_midnight
    period_start_local_time = execution_start_date_local_time_midnight - relativedelta(years=4)

    def mock_read_table(*args, **kwargs):
        return spark.createDataFrame(
            data=[
                (  # Outside period: A day after start time
                    "100000000000000001",
                    execution_start_datetime_utc + relativedelta(days=1),
                    Decimal("1.123"),
                    QuantityQuality.MEASURED.value,
                    MeteringPointType.CONSUMPTION.value,
                    1,
                    execution_start_datetime_local_time.year,
                    execution_start_datetime_local_time.month,
                ),
                (  # Inside period: Exactly at start time
                    "100000000000000002",
                    execution_start_datetime_utc,
                    Decimal("1.123"),
                    QuantityQuality.MEASURED.value,
                    MeteringPointType.CONSUMPTION.value,
                    2,
                    execution_start_datetime_local_time.year,
                    execution_start_datetime_local_time.month,
                ),
                (  # Inside period: Exactly at end time
                    "100000000000000003",
                    period_start_local_time,
                    Decimal("1.123"),
                    QuantityQuality.MEASURED.value,
                    MeteringPointType.CONSUMPTION.value,
                    3,
                    execution_start_datetime_local_time.year,
                    execution_start_datetime_local_time.month,
                ),
                (  # Ouside period: A day after end time
                    "100000000000000004",
                    period_start_local_time - relativedelta(days=1),
                    Decimal("1.123"),
                    QuantityQuality.MEASURED.value,
                    MeteringPointType.CONSUMPTION.value,
                    4,
                    execution_start_datetime_local_time.year,
                    execution_start_datetime_local_time.month,
                ),
            ],
            schema=CURRENT_MEASUREMENTS_SCHEMA,
        )

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Act
    actual = current_measurements_repository.read_current_measurements(
        period_start_utc=period_start_local_time.astimezone(UTC),
        period_end_utc=period_end_local_time.astimezone(UTC),
    )

    # Assert
    actual_ids = [row.metering_point_id for row in actual.df.collect()]
    assert actual_ids == ["100000000000000002", "100000000000000003"]


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

    def mock_read_table(*args, **kwargs):
        return spark.createDataFrame(
            data=[
                (  # Inside: Metering point id included
                    "100000000000000001",
                    period_start_utc,
                    Decimal("1.123"),
                    QuantityQuality.MEASURED.value,
                    MeteringPointType.CONSUMPTION.value,
                    1,
                    period_start_utc.astimezone(ZoneInfo("Europe/Copenhagen")).year,
                    period_start_utc.astimezone(ZoneInfo("Europe/Copenhagen")).month,
                ),
                (  # Outside: Metering point id not included
                    "100000000000000002",
                    period_start_utc,
                    Decimal("1.123"),
                    QuantityQuality.MEASURED.value,
                    MeteringPointType.CONSUMPTION.value,
                    2,
                    period_start_utc.astimezone(ZoneInfo("Europe/Copenhagen")).year,
                    period_start_utc.astimezone(ZoneInfo("Europe/Copenhagen")).month,
                ),
            ],
            schema=CURRENT_MEASUREMENTS_SCHEMA,
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
    assert actual_ids == ["100000000000000001"]


def test__period_filtering_during_dst_change(
    spark: SparkSession,
    current_measurements_repository: CurrentMeasurementsRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    period_start_utc = datetime(2024, 3, 31, 0, 0, tzinfo=UTC)  # DST change day in Europe/Copenhagen
    period_end_utc = datetime(2024, 3, 31, 4, 0, tzinfo=UTC)

    def mock_read_table(*args, **kwargs):
        return spark.createDataFrame(
            data=[
                # Just before DST change
                (
                    "100000000000000001",
                    datetime(2024, 3, 31, 0, 59, tzinfo=UTC),  # 01:59 Europe/Copenhagen
                    Decimal("1.123"),
                    QuantityQuality.MEASURED.value,
                    MeteringPointType.CONSUMPTION.value,
                    1,
                    2024,
                    3,
                ),
                # Right after DST change
                (
                    "100000000000000002",
                    datetime(2024, 3, 31, 1, 0, tzinfo=UTC),  # 03:00 Europe/Copenhagen
                    Decimal("2.456"),
                    QuantityQuality.MEASURED.value,
                    MeteringPointType.CONSUMPTION.value,
                    2,
                    2024,
                    3,
                ),
                # Outside range
                (
                    "100000000000000003",
                    datetime(2024, 3, 31, 4, 1, tzinfo=UTC),  # 06:01 Europe/Copenhagen
                    Decimal("3.789"),
                    QuantityQuality.MEASURED.value,
                    MeteringPointType.CONSUMPTION.value,
                    3,
                    2024,
                    3,
                ),
            ],
            schema=CURRENT_MEASUREMENTS_SCHEMA,
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
