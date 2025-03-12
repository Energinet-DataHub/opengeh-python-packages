# GEH Common Release Notes

## Version 5.3.4

- Added more valid types to `source/geh_common/src/geh_common/domain/types/`

## Version 5.3.3

- Added more valid types to `source/geh_common/src/geh_common/domain/types/`

## Version 5.3.2

- Refactored project to use new Github Actions CI/CD pipelines
- Moved project to `source` to fit organization repository structure

## Version 5.3.1

**Subpackage**: `geh_common.databricks`

- Add `cancel_job_run`. The function waits for the job run to be cancelled by default.
- Add `get_latest_job_run_id` to get the latest run id for a Databricks job.

## Version 5.3.0

**Subpackage**: `geh_common.databricks`

### Breaking Changes

- Moved `databricks_api_client` from `testing.container_test` to `databricks`.
- Renamed `seed` method in `databricks_api_client` to `execute_statement`.
- Removed `catalog` and `schema` from `execute_statement` method. This can be set in the statement if neccessary.

## Version 5.2.0

**Subpackage**: `geh_common.telemetry`

- Updated logging setup to use Pydantic Settings class and introduced start_trace decorator to simplify logging setup

## Version 5.0.1

**Subpackage**: `geh_common.migrations`

Bug fix:

- The incorrect import caused the consumer build to fail

## Version 5.0.0

**Subpackage**: `geh_common.migrations`

Following changes were made to simplify the package's usage and reduce the consumer's need to understand its
implementation details

- Removed the use of `create_and_configure_container` for the consumer.
- The `migrate` function now takes `SparkSqlMigrationsConfigurations` as a parameter.

## Version 3.0.0

**Subpackage**: `geh_common.migrations`

Breaking changes

The `spark sql migrations` package is being simplified by removing the current state and rollback functionalities.

The current state functionality, which was mainly used by the migration team to easily recreate deleted tables when data needed to be rerun, has been removed. If you still need this functionality, please use version 2.0.6.

The rollback functionality has been removed due to the risk of unintended rollbacks. It has been decided that any issues during migration should be handled manually.

- Removed current_state functionality
- Removed rollback functionality
- Removed `schema_config` parameter from `SparkSqlMigrationsConfiguration` class

These changes are made to simplify the database migration

## Version 2.4.1

**Subpackage**: `geh_common.telemetry`

- Change required version to ranges

## Version 2.4.0

**Subpackage**: `geh_common.telemetry`

- Added integration test for telemetry

## Version 2.1.2

**Subpackage**: `geh_common.telemetry`

- Fix

## Version 2.1.1

**Subpackage**: `geh_common.telemetry`

- Open minor version for telemetry

## Version 2.1.0

**Subpackage**: `geh_common.telemetry`

- Added shared telemetry library for logging

## Version 2.0.6

**Subpackage**: `geh_common.migrations`

- Added temporary fix that allows migration without current state scripts.

## Version 2.0.5

**Subpackage**: `geh_common.migrations`

- Change minimum required version of `pyspark` to `3.5.1`

## Version 2.0.4

**Subpackage**: `geh_common.migrations`

- Fix

## Version 2.0.3

**Subpackage**: `geh_common.migrations`

- open minor version for spark_sql_migrations

## Version 2.0.2

**Subpackage**: `geh_common.migrations`

- required pyspark version bumped form 3.5.1 to 3.5.3
- required dependency_injector version bumped from 4.41.0 to 4.43.0

## Version 2.0.1

**Subpackage**: `geh_common.migrations`

- Bug fix: When rollback is enabled, it needs to get the latest version of each table before executing the migration.
However, when getting the version it was collecting all the rows from the history, which was not needed and could
cause a performance issue. Now it is only getting the latest version of each table.

## Version 2.0.0

**Subpackage**: `geh_common.migrations`

- Added `rollback_on_failure` parameter to the `SparkSqlMigrationsConfiguration` class. This parameter allows the user to specify whether the migration should be rolled back if an error occurs during the migration process. The default value is `False`.
<br> The reason for this change is that a rollback might unintentionally delete data that was not supposed to be deleted. The situation can occur when data are written to the table at the same time that the migration is executing. The user should be aware of the risks of rolling back a migration and should only do so if they are sure that the migration will not cause any data loss.

### Breaking Changes

As the behavior before was `True`, the default value is now `False`. This means that if an error occurs during the migration process, the migration will not be rolled back by default. If the user wants to roll back the migration in case of an error, they can set the `rollback_on_failure` parameter to `True`.

Example:

``` python
spark_config = SparkSqlMigrationsConfiguration(
    ...
    rollback_on_failure=True
 )
```

## Version 1.9.5

**Subpackage**: `geh_common.telemetry`

- Added Release Notes

## Version 1.9.4

**Subpackage**: `geh_common.migrations`

- Bug fixed for rollback when more than two migration scripts are running at the same time.

## Version 1.9.3

**Subpackage**: `geh_common.migrations`

- SparkSqlMigrationsConfiguration no longer defaults to hive_metastore as catalog name, so now it is not an optional property.

### Changes

The `catalog_name` parameter in the `SparkSqlMigrationsConfiguration` class is no longer optional. The default value was `hive_metastore`, but now it is required to be set by the user.

Example:

``` python
spark_config = SparkSqlMigrationsConfiguration(
    ...
    catalog_name="some_catalog_name"
 )
```

## Version 1.9.2

**Subpackage**: `geh_common.migrations`

- Adding release notes

## Version 1.9.1

**Subpackage**: `geh_common.migrations`

- Adding schema to the Views.

### Changes

Every View in the `schema_config` file can now have a schema defined. It is optional for now.

It can be used to test that the views have the expected schema with unit tests.

Example:

``` python
  views=[
      View(name="test_view", schema=test_view_schema)
  ]
```
