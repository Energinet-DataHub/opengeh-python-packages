from unittest.mock import patch

import opengeh_utilities.migrations.infrastructure.apply_migration_scripts as apply_migrations
import opengeh_utilities.migrations.infrastructure.uncommitted_migration_scripts as uncommitted_migrations
import opengeh_utilities.migrations.migration_pipeline as sut


def test__migrate__when_no_uncommitted_migrations__should_not_call_apply_migrations() -> (
    None
):
    # Arrange
    with patch(
        "opengeh_utilities.migrations.migration_pipeline.apply_migrations"
    ) as mocked_apply_migrations:
        with patch(
            "opengeh_utilities.migrations.migration_pipeline.uncommitted_migrations"
        ) as mocked_uncommitted_migrations:
            mocked_uncommitted_migrations.get_uncommitted_migration_scripts.return_value = []

            # Act
            sut.migrate()

            # Assert
            mocked_apply_migrations.apply_migration_scripts.assert_not_called()


def test__migrate__when_uncommitted_migrations_not_zero__should_call_apply_migrations() -> (
    None
):
    # Arrange
    with patch(
        "opengeh_utilities.migrations.migration_pipeline.apply_migrations"
    ) as mocked_apply_migrations:
        with patch(
            "opengeh_utilities.migrations.migration_pipeline.uncommitted_migrations"
        ) as mocked_uncommitted_migrations:
            mocked_uncommitted_migrations.get_uncommitted_migration_scripts.return_value = [
                "test_migration"
            ]
            mocked_apply_migrations.apply_migration_scripts.side_effect = None

            # Act
            sut.migrate()

            # Assert
            mocked_apply_migrations.apply_migration_scripts.assert_called_once()
