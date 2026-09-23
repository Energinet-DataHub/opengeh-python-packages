from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel

from geh_common.testing.dataframes.assert_dataframes import (
    AssertDataframesConfiguration,
    assert_dataframes_and_schemas,
    assert_dataframes_equal,
)


def _dataframes_with_excess_counts(actual_excess_count: int = 0, expected_excess_count: int = 0):
    actual = MagicMock(spec=DataFrame)
    expected = MagicMock(spec=DataFrame)
    actual_excess = MagicMock(spec=DataFrame)
    expected_excess = MagicMock(spec=DataFrame)
    actual.subtract.return_value = actual_excess
    expected.subtract.return_value = expected_excess
    actual_excess.count.return_value = actual_excess_count
    expected_excess.count.return_value = expected_excess_count
    return actual, expected


def test_when_counts_are_not_supplied_then_dataframe_counts_are_used():
    actual, expected = _dataframes_with_excess_counts()
    actual.count.return_value = 3
    expected.count.return_value = 3

    assert_dataframes_equal(actual, expected)

    actual.count.assert_called_once_with()
    expected.count.assert_called_once_with()


def test_when_counts_are_supplied_then_dataframe_counts_are_not_recomputed():
    actual, expected = _dataframes_with_excess_counts()

    assert_dataframes_equal(actual, expected, actual_count=3, expected_count=3)

    actual.count.assert_not_called()
    expected.count.assert_not_called()


@patch("geh_common.testing.dataframes.assert_dataframes.assert_dataframes_equal")
@patch("geh_common.testing.dataframes.assert_dataframes.assert_schema")
def test_assert_dataframes_and_schemas_reuses_computed_counts(assert_schema, assert_equal):
    actual = MagicMock(spec=DataFrame)
    expected = MagicMock(spec=DataFrame)
    actual.count.return_value = 3
    expected.count.return_value = 3
    configuration = AssertDataframesConfiguration(
        ignore_extra_columns_in_actual=False,
        ignore_duplicated_rows=True,
    )

    assert_dataframes_and_schemas(actual, expected, configuration)

    actual.count.assert_called_once_with()
    expected.count.assert_called_once_with()
    actual.persist.assert_called_once_with(StorageLevel.MEMORY_AND_DISK)
    expected.persist.assert_called_once_with(StorageLevel.MEMORY_AND_DISK)
    actual.unpersist.assert_called_once_with()
    expected.unpersist.assert_called_once_with()
    assert_schema.assert_called_once()
    assert_equal.assert_called_once_with(actual, expected, actual_count=3, expected_count=3)


@patch("geh_common.testing.dataframes.assert_dataframes.assert_dataframes_equal")
@patch("geh_common.testing.dataframes.assert_dataframes.assert_schema")
def test_assert_dataframes_and_schemas_unpersists_dataframes_on_assertion_failure(assert_schema, assert_equal):
    actual = MagicMock(spec=DataFrame)
    expected = MagicMock(spec=DataFrame)
    actual.count.return_value = 3
    expected.count.return_value = 3
    assert_equal.side_effect = AssertionError("Dataframes data are not equal")
    configuration = AssertDataframesConfiguration(
        show_columns_when_actual_and_expected_are_equal=True,
        ignore_extra_columns_in_actual=False,
        ignore_duplicated_rows=True,
    )

    with pytest.raises(AssertionError, match="Dataframes data are not equal"):
        assert_dataframes_and_schemas(actual, expected, configuration)

    actual.unpersist.assert_called_once_with()
    expected.unpersist.assert_called_once_with()


@patch("geh_common.testing.dataframes.assert_dataframes.assert_dataframes_equal")
@patch("geh_common.testing.dataframes.assert_dataframes.assert_schema")
def test_assert_dataframes_and_schemas_persists_same_dataframe_once(assert_schema, assert_equal):
    dataframe = MagicMock(spec=DataFrame)
    dataframe.count.return_value = 3
    configuration = AssertDataframesConfiguration(
        ignore_extra_columns_in_actual=False,
        ignore_duplicated_rows=True,
    )

    assert_dataframes_and_schemas(dataframe, dataframe, configuration)

    dataframe.persist.assert_called_once_with(StorageLevel.MEMORY_AND_DISK)
    dataframe.unpersist.assert_called_once_with()


def test_when_supplied_counts_differ_then_raises_assertion_error():
    actual, expected = _dataframes_with_excess_counts()

    with pytest.raises(AssertionError, match="Dataframes data are not equal"):
        assert_dataframes_equal(actual, expected, actual_count=3, expected_count=2)


def test_when_rows_differ_then_raises_assertion_error():
    actual, expected = _dataframes_with_excess_counts(actual_excess_count=1)

    with pytest.raises(AssertionError, match="Dataframes data are not equal"):
        assert_dataframes_equal(actual, expected, actual_count=3, expected_count=3)
