from unittest.mock import Mock
import spark_sql_migrations.migration_pipeline as sut
import spark_sql_migrations.infrastructure.uncommitted_migration_scripts as uncommitted_migrations
import spark_sql_migrations.infrastructure.apply_migration_scripts as apply_migrations


def test__migrate__when_no_uncommitted_migrations__should_not_call_apply_migrations(
    mocker: Mock,
) -> None:
    # Arrange
    mocker.patch.object(
        uncommitted_migrations,
        uncommitted_migrations.get_uncommitted_migration_scripts.__name__,
        return_value=[],
    )
    mocked_apply_migrations = mocker.patch.object(
        apply_migrations,
        apply_migrations.apply_migration_scripts.__name__,
    )

    # Act
    sut.migrate()

    # Assert
    mocked_apply_migrations.assert_not_called()


def test__migrate__when_uncommitted_migrations_not_zero__should_call_apply_migrations(
    mocker: Mock,
) -> None:
    # Arrange
    mocker.patch.object(
        uncommitted_migrations,
        uncommitted_migrations.get_uncommitted_migration_scripts.__name__,
        return_value=["test_migration"],
    )
    mocked_apply_migrations = mocker.patch.object(
        apply_migrations,
        apply_migrations.apply_migration_scripts.__name__,
        side_effect=None,
    )

    # Act
    sut.migrate()

    # Assert
    mocked_apply_migrations.assert_called_once()
