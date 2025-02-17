import geh_common.migrations.infrastructure.apply_migration_scripts as apply_migrations
import geh_common.migrations.infrastructure.uncommitted_migration_scripts as uncommitted_migrations


def migrate() -> None:
    migrations: list[str] = uncommitted_migrations.get_uncommitted_migration_scripts()
    if len(migrations) > 0:
        (apply_migrations.apply_migration_scripts(migrations))
