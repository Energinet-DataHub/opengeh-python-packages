import builtins
import shutil
import sys
from typing import Generator

import pytest
from pyspark.sql import SparkSession

from geh_common.testing.spark.spark_test_session import get_spark_test_session


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession):
    yield
    # Clear the cache after each test module to avoid memory issues
    spark.catalog.clearCache()


_spark, data_dir = get_spark_test_session()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session with Delta Lake enabled.
    """
    yield _spark
    _spark.stop()
    shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture(scope="session", autouse=True)
def original_print():
    """
    pytest-xdist disables stdout capturing by default, which means that print() statements
    are not captured and displayed in the terminal.
    That's because xdist cannot support -s for technical reasons wrt the process execution mechanism
    https://github.com/pytest-dev/pytest-xdist/issues/354
    """
    original_print = print
    with pytest.MonkeyPatch.context() as m:
        m.setattr(builtins, "print", lambda *args, **kwargs: original_print(*args, **{"file": sys.stderr, **kwargs}))
        yield original_print
        m.undo()
