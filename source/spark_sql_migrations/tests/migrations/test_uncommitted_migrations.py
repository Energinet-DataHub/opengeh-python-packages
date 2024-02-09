import tests.helpers.table_helper as table_helper
import spark_sql_migrations.migrations.uncommitted_migrations as sut
from unittest.mock import Mock
from pyspark.sql import SparkSession
from importlib.resources import contents
from tests.helpers.spark_helper import reset_spark_catalog
from spark_sql_migrations.schemas.migrations_schema import schema_migration_schema
from tests.helpers.schema_migration_costants import SchemaMigrationConstants

storage_account = "storage_account"


def test_get_committed_migrations_when_no_table_exists_returns_empty_list(
    spark: SparkSession,
) -> None:
    # Arrange
    reset_spark_catalog(spark)

    # Act
    actual = sut._get_committed_migrations()

    # Assert
    assert len(actual) == 0


def test_get_committed_migrations_when_no_table_exists_creates_schema_migration_table(
    spark: SparkSession
) -> None:
    # Arrange
    reset_spark_catalog(spark)

    # Act
    sut._get_committed_migrations()

    # Assert
    assert spark.catalog.tableExists(
        SchemaMigrationConstants.table_name, SchemaMigrationConstants.schema_name
    )


def test_get_committed_migration_when_table_exists_returns_rows(
    spark: SparkSession
) -> None:
    # Arrange
    reset_spark_catalog(spark)
    table_helper.create_schema_and_table(
        spark,
        SchemaMigrationConstants.schema_name,
        SchemaMigrationConstants.table_name,
        schema_migration_schema
    )

    spark.sql(
        f"""INSERT INTO {SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}
        VALUES ('test_script', current_timestamp())"""
    )

    # Act
    actual = sut._get_committed_migrations()

    # Assert
    assert len(actual) == 1


def test_get_all_migrations_returns_expected_migrations(mocker: Mock) -> None:
    # Arrange
    mocker.patch.object(
        sut,
        contents.__name__,
        return_value=["migration2.sql", "migration1.sql", "__init__.py"],
    )
    expected_migrations = ["migration1", "migration2"]

    # Act
    actual = sut.get_all_migrations()

    # Assert
    assert actual == expected_migrations


def test_get_uncommitted_migrations_when_no_migrations_needed_return_zero(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    migration1 = "migration1"
    migration2 = "migration2"

    mocker.patch.object(sut, sut.get_all_migrations.__name__, return_value=[migration1, migration2])
    mocker.patch.object(sut, sut._get_committed_migrations.__name__, return_value=[migration1, migration2])

    # Act
    actual = sut.get_uncommitted_migrations()

    # Assert
    assert len(actual) == 0


def test_get_uncommitted_migrations_when_one_migrations_needed_return_one(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    migration1 = "migration1"
    migration2 = "migration2"

    mocker.patch.object(sut, sut.get_all_migrations.__name__, return_value=[migration1, migration2])
    mocker.patch.object(sut, sut._get_committed_migrations.__name__, return_value=[migration1])

    # Act
    actual = sut.get_uncommitted_migrations()

    # Assert
    assert len(actual) == 1


def test_get_uncommitted_migrations_when_multiple_migrations_return_in_correct_order(
    mocker: Mock, spark: SparkSession
) -> None:
    # Arrange
    migration1 = "202311100900_migration_1"
    migration2 = "202311200900_migration_2"

    mocker.patch.object(sut, sut.get_all_migrations.__name__, return_value=[migration1, migration2])
    mocker.patch.object(sut, sut._get_committed_migrations.__name__, return_value=[])

    # Act
    actual = sut.get_uncommitted_migrations()

    # Assert
    assert actual[0] == migration1
    assert actual[1] == migration2


def test_create_schema_migration_table(spark: SparkSession) -> None:
    # Arrange
    reset_spark_catalog(spark)

    # Act
    sut._create_schema_migration_table(
        SchemaMigrationConstants.schema_name,
        SchemaMigrationConstants.table_name,
    )

    # Assert
    assert spark.catalog.tableExists(
        SchemaMigrationConstants.table_name, SchemaMigrationConstants.schema_name
    )
