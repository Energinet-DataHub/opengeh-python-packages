from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

import opengeh_common.migrations.infrastructure.uncommitted_migration_scripts as sut
import tests.unit.migrations.helpers.spark_helper as spark_helper
import tests.unit.migrations.helpers.table_helper as table_helper
from opengeh_common.migrations.schemas.migrations_schema import (
    schema_migration_schema,
)
from tests.unit.migrations.helpers.schema_migration_costants import (
    SchemaMigrationConstants,
)

storage_account = "storage_account"


def test__get_committed_migrations__when_no_table_exists__return_empty_list(
    spark: SparkSession,
) -> None:
    # Arrange
    spark_helper.drop_table(
        spark,
        SchemaMigrationConstants.catalog_name,
        SchemaMigrationConstants.schema_name,
        SchemaMigrationConstants.table_name,
    )
    table_helper.create_schema(
        spark,
        SchemaMigrationConstants.catalog_name,
        SchemaMigrationConstants.schema_name,
    )

    # Act
    actual = sut._get_committed_migration_scripts()

    # Assert
    assert len(actual) == 0


def test__get_committed_migrations__when_no_table_exists__create_schema_migration_table(
    spark: SparkSession,
) -> None:
    # Arrange
    spark_helper.reset_spark_catalog(spark)
    table_helper.create_schema(
        spark,
        SchemaMigrationConstants.catalog_name,
        SchemaMigrationConstants.schema_name,
    )

    # Act
    sut._get_committed_migration_scripts()

    # Assert
    assert spark.catalog.tableExists(
        f"{SchemaMigrationConstants.catalog_name}.{SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}"
    )


def test__get_committed_migration__when_table_exists__returns_rows(
    spark: SparkSession,
) -> None:
    # Arrange
    spark_helper.reset_spark_catalog(spark)
    table_helper.create_schema_and_table(
        spark,
        SchemaMigrationConstants.catalog_name,
        SchemaMigrationConstants.schema_name,
        SchemaMigrationConstants.table_name,
        schema_migration_schema,
    )

    spark.sql(
        f"""INSERT INTO {SchemaMigrationConstants.catalog_name}.{SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}
        VALUES ('test_script', current_timestamp())"""
    )

    # Act
    actual = sut._get_committed_migration_scripts()

    # Assert
    assert len(actual) == 1


def test__get_all_migrations__returns_expected_migrations() -> None:
    # Arrange
    class _MockFiles:
        def __init__(self, _) -> None:
            pass

        def iterdir(self):
            return [
                Path("migration2.sql"),
                Path("migration1.sql"),
                Path("__init__.py"),
            ]

    with patch(
        "opengeh_utilities.migrations.infrastructure.uncommitted_migration_scripts.files",
        _MockFiles,
    ):
        expected_migrations = ["migration1", "migration2"]

        # Act
        actual = sut.get_all_migration_scripts()

        # Assert
        assert actual == expected_migrations


def test__get_uncommitted_migrations__when_no_migrations_needed__return_zero() -> None:
    # Arrange
    migration1 = "migration1"
    migration2 = "migration2"

    with patch(
        "opengeh_utilities.migrations.infrastructure.uncommitted_migration_scripts.get_all_migration_scripts"
    ) as get_all_migration_scripts:
        get_all_migration_scripts.return_value = [migration1, migration2]
        with patch(
            "opengeh_utilities.migrations.infrastructure.uncommitted_migration_scripts._get_committed_migration_scripts"
        ) as get_committed_migration_scripts:
            get_committed_migration_scripts.return_value = [migration1, migration2]

            # Act
            actual = sut.get_uncommitted_migration_scripts()

            # Assert
            assert len(actual) == 0


def test__get_uncommitted_migrations__when_one_migrations_needed__return_one() -> None:
    # Arrange
    migration1 = "migration1"
    migration2 = "migration2"

    patch.object(
        sut,
        sut.get_all_migration_scripts.__name__,
        return_value=[migration1, migration2],
    )
    patch.object(
        sut, sut._get_committed_migration_scripts.__name__, return_value=[migration1]
    )

    # Act
    actual = sut.get_uncommitted_migration_scripts()

    # Assert
    assert len(actual) == 1


def test__get_uncommitted_migrations__when_multiple_migrations__return_in_correct_order() -> (
    None
):
    # Arrange
    migration1 = "202311100900_migration_1"
    migration2 = "202311200900_migration_2"

    with patch(
        "opengeh_utilities.migrations.infrastructure.uncommitted_migration_scripts.get_all_migration_scripts"
    ) as get_all_migration_scripts:
        get_all_migration_scripts.return_value = [migration1, migration2]
        with patch(
            "opengeh_utilities.migrations.infrastructure.uncommitted_migration_scripts._get_committed_migration_scripts"
        ) as get_committed_migration_scripts:
            get_committed_migration_scripts.return_value = []

            # Act
            actual = sut.get_uncommitted_migration_scripts()

            # Assert
            assert actual[0] == migration1
            assert actual[1] == migration2


def test__create_schema_migration_table__when_table_does_not_exist__create_schema_migration_table(
    spark: SparkSession,
) -> None:
    # Arrange
    spark_helper.reset_spark_catalog(spark)
    table_helper.create_schema(
        spark,
        SchemaMigrationConstants.catalog_name,
        SchemaMigrationConstants.schema_name,
    )

    # Act
    sut._create_schema_migration_table(
        SchemaMigrationConstants.schema_name,
        SchemaMigrationConstants.table_name,
    )

    # Assert
    assert spark.catalog.tableExists(
        f"{SchemaMigrationConstants.catalog_name}.{SchemaMigrationConstants.schema_name}.{SchemaMigrationConstants.table_name}"
    )
