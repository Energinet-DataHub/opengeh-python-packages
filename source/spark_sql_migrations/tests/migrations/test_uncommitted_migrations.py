import package.schema_migration.uncommitted_migrations as sut
from importlib.resources import contents
from unittest.mock import patch, Mock
from pyspark.sql import SparkSession
from package.constants.schema_migration import SchemaMigrationConstants
from tests.schema_migration.schema_migration_helper import reset_migration_table
import tests.helpers.mock_helper as mock_helper

storage_account = "storage_account"


def test_get_all_migrations_returns_some() -> None:
    # Act
    actual = sut.get_all_migrations()

    # Assert: This test will fail if there are in fact no migrations
    assert len(actual) > 0


@patch.object(
    sut.path_helper,
    sut.path_helper.get_storage_base_path.__name__,
)
def test_get_committed_migrations_when_no_table_exists_returns_empty_list(
    spark: SparkSession,
) -> None:
    # Arrange
    spark.sql(
        f"DROP TABLE IF EXISTS {SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}"
    )

    # Act
    actual = sut._get_committed_migrations()

    # Assert
    assert len(actual) == 0


@patch.object(
    sut.path_helper,
    sut.path_helper.get_storage_base_path.__name__,
)
def test_get_committed_migrations_when_no_table_exists_creates_schema_migration_table(
    mock_path_helper: Mock, spark: SparkSession
) -> None:
    # Arrange
    mock_path_helper.side_effect = mock_helper.base_path_helper
    spark.sql(
        f"DROP TABLE IF EXISTS {SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}"
    )

    # Act
    sut._get_committed_migrations()

    # Assert
    assert spark.catalog.tableExists(
        SchemaMigrationConstants.table_name, SchemaMigrationConstants.schema_name
    )


@patch.object(
    sut.path_helper,
    sut.path_helper.get_storage_base_path.__name__,
)
def test_get_committed_migration_when_table_exists_returns_rows(
    mock_path_helper: Mock, spark: SparkSession
) -> None:
    # Arrange
    mock_path_helper.side_effect = mock_helper.base_path_helper
    reset_migration_table(spark)
    spark.sql(
        f"""INSERT INTO {SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}
        VALUES ('test_script', current_timestamp())"""
    )

    # Act
    actual = sut._get_committed_migrations()

    # Assert
    assert len(actual) == 1


@patch.object(sut, contents.__name__)
def test_get_all_migrations_returns_expected_migrations(mock_contents: Mock) -> None:
    # Arrange
    mock_contents.return_value = ["migration2.sql", "migration1.sql", "__init__.py"]
    expected_migrations = ["migration1", "migration2"]

    # Act
    actual = sut.get_all_migrations()

    # Assert
    assert actual == expected_migrations


@patch.object(sut, sut.get_all_migrations.__name__)
@patch.object(sut, sut._get_committed_migrations.__name__)
def test_get_uncommitted_migrations_when_no_migrations_needed_return_zero(
    mock_all_migrations: Mock, mock_committed_migrations: Mock, spark: SparkSession
) -> None:
    # Arrange
    migration1 = "migration1"
    migration2 = "migration2"

    mock_all_migrations.return_value = [migration1, migration2]
    mock_committed_migrations.return_value = [migration1, migration2]

    # Act
    actual = sut.get_uncommitted_migrations()

    # Assert
    assert len(actual) == 0


@patch.object(sut, sut._get_committed_migrations.__name__)
@patch.object(sut, sut.get_all_migrations.__name__)
def test_get_uncommitted_migrations_when_one_migrations_needed_return_one(
    mock_all_migrations: Mock, mock_committed_migrations: Mock, spark: SparkSession
) -> None:
    # Arrange
    migration1 = "migration1"
    migration2 = "migration2"

    mock_all_migrations.return_value = [migration1, migration2]
    mock_committed_migrations.return_value = [migration1]

    # Act
    actual = sut.get_uncommitted_migrations()

    # Assert
    assert len(actual) == 1


@patch.object(sut, sut._get_committed_migrations.__name__)
@patch.object(sut, sut.get_all_migrations.__name__)
def test_get_uncommitted_migrations_when_multiple_migrations_return_in_correct_order(
    mock_all_migrations: Mock, mock_committed_migrations: Mock, spark: SparkSession
) -> None:
    # Arrange
    migration1 = "202311100900_migration_1"
    migration2 = "202311200900_migration_2"

    mock_all_migrations.return_value = [migration2, migration1]
    mock_committed_migrations.return_value = []

    # Act
    actual = sut.get_uncommitted_migrations()

    # Assert
    assert actual[0] == migration1
    assert actual[1] == migration2


@patch.object(
    sut.path_helper,
    sut.path_helper.get_storage_base_path.__name__,
)
def test_create_schema_migration_table(mock_path_helper: Mock, spark: SparkSession) -> None:
    # Arrange
    mock_path_helper.side_effect = mock_helper.base_path_helper
    spark.sql(
        f"DROP TABLE IF EXISTS {SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}"
    )

    # Act
    sut._create_schema_migration_table(
        spark,
        SchemaMigrationConstants.default_location,
        SchemaMigrationConstants.schema_name,
        SchemaMigrationConstants.table_name,
    )

    # Assert
    assert spark.catalog.tableExists(
        SchemaMigrationConstants.table_name, SchemaMigrationConstants.schema_name
    )
