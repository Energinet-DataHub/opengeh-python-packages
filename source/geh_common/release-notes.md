# GEH Common Release Notes

## Version 6.4.2

- Added `convert_datetime_to_midnight_in_timezone` function for converting a datetime into mightnight in a given timezone

## Version 6.4.1

Bug fix: Implement overwrite in `geh_common.testing.dataframes.write_to_delta.write_when_files_to_delta`
as delete all rows and then append new rows. This preserves delta table meta data like clustering.

## Version 6.4.0

Added measurements-calculated data products:

- `calculated_measurements_including_test_data_v1`
- `missing_measurements_log_including_test_data_v1`

## Version 6.3.0

Adding optimizations functions for Databricks

- Added `optimize_table` function
- Added `analyze_table` function
- Added `vacuum_table` function

## Version 6.2.1

- Add `measurements_current_v2` schema

## Version 6.2.0

- Added `MeteringPointIds`type

## Version 6.1.4

- Update contracts for electrical heating and net consumption periodisations

## Version 6.1.3

- Update measurements_current_v1 schema

## Version 6.1.2

- Add measurements_current_v1 schema

## Version 6.1.1

- Electricity market report input contract update

## Version 6.1.0

- Add ignore_row_order default true to assert_dataframes_and_schemas, so it is possible to test row order

## Version 6.0.0

- Set `cli_ignore_unknown_args=False` by default in the ApplicationSettings class

## Version 5.14.1

- Extended energy supplier ID validation rule (GLN can only consist of digits and EIC may consist of number and letters).

## Version 5.14.0

- Added `datetime_format` parameter to `read_csv` for specifying the csv files datetime format.

## Version 5.13.1

Fix import bug that prevented using the `@retry_policy` decorator.

## Version 5.13.0

Add decorator `@retry_policy` to declaratively apply exponential backoff retry policy on functions and methods.

Example:

```python
        @retry_policy(delay=2, retries=5)
        def unreliable_function():
            # function implementation
```

## Version 5.12.0

- Add ignore_duplicated_rows to assert_dataframes, default=False

## Version 5.11.0

- Adds MeteringPointTypes and validation

## Version 5.10.13

- Adds check in `TestCase` to ensure that `then` files follow the schema of the `actual` data frame.

## Version 5.10.12

- Add optional seperator parameter to TestCase, default is ";"

## Version 5.10.11

- Removed data contract for `measurements_core.measurements_gold.sap_delta_v1`

## Version 5.10.10

- Add `transaction_start_time` and `transaction_end_time` to `measurements_calculated.calculated_measurements_v1` contract.

## Version 5.10.9

- Updated description for data product contract for `electrical_heating_child_metering_points_v1`.

## Version 5.10.8

- Changes `geh_common.infrastructure.write_csv._merge_content` to use `set` instead of `list` to ensure that we don't merge the same file multiple times.

## Version 5.10.7

- Fixes a bug in `geh_common.infrastructure.write_csv._merge_content` where it overwrote data in the temporary files.

## Version 5.10.6

- Removes check in `_write_dataframe` where it would fail if the dataframe was empty.

## Version 5.10.5

- Adds `EnergySupplierIds` type for validating energy supplier IDs in pydantic models.
- Refactors logic for `GridAreaCodes` as converters can be shared between different types.
- Adds tests for `EnergySupplierIds` to ensure correct validation and conversion.

## Version 5.10.4

- `physical_status` should be a string not a boolean

## Version 5.10.3

- Added data contract for `measurements_core.measurements_gold.sap_delta_v1`
- Changed quantity to nullable for `measurements_core.measurements_gold.current_v1` as it is NULL when quality = missing

## Version 5.10.2

- Added data contract for `{CATALOG}.electricity_market_reports_input.measurement_report_metering_point_periods_v1`

## Version 5.10.1

- `get_spark_test_session` returns a `Path` not a `str` for the data directory.

## Version 5.10.0

- Implements a generalized version of the `ZipTask` from `geh_settlement_report`.
    - This involves two functions: `create_zip_file` and `write_csv_files`.

## Version 5.9.4

- Update documentation of missing measurements log contract

## Version 5.9.3

- add project script test

## Version 5.9.2

- Update to contract text for net_consumption_group_6_child_metering_points_v1 and net_consumption_group_6_consumption_metering_point_periods_v1

## Version 5.9.1

- More documentation of missing measurements log contract

## Version 5.9.0

- Added project script test functionality

## Version 5.8.14

- Added `geh_common.testing.dataframes.assert_table.assert_table_properties`

## Version 5.8.13

- Deactivates streamlit app in the `geh_common.covernator_streamlit` subpackage. Streamlit requires a version of ProtoBuf that is not compatible with the current version used by measurements `core`.

## Version 5.8.12

- Added hive support for persisting migrations scripts

## Version 5.8.11

- Added covernator functionality to write to github output with statistics

## Version 5.8.10

- Adding functionally to execute_statement, such that it can handle both sync and async calls.

## Version 5.8.9

Add `geh_common.pyspark.clamp` functions to clamp periods.

## Version 5.8.8

- Removed set notation characters from nsg6 consumption contract

## Version 5.8.7

- Improves docstring for `get_spark_test_session` function in `geh_common.testing` subpackage. Now the origin of the configurations used should be apparent.

## Version 5.8.6

- Fixed a typo in Data Product definition
- Added __init__.py files to Data Products

## Version 5.8.5

- minor adjustments to databricks api to ensure to runs Synchronously

## Version 5.8.4

- Fixed input paramter in execute_statement databricks api method.

## Version 5.8.3

Modified the electrical_heating contract

- Removed the `has_electrical_heating` column
- Allows for `net_settlement_group` group 1
- Removed the set notation characters

## Version 5.8.2

- explicitly sort test results for covernator to avoid failing tests
- ignore empty dataframes to handle polars concatenation error

## Version 5.8.1

- Databricks api makeover.

## Version 5.8.0

Added new **Subpackage**: `geh_common.covernator_streamlit`

### CLI Setup & Usage

Add the following to the pyproject.toml in the repository that uses covernator to scan QA tests and test coverage:

```toml
[project.scripts]
covernator = "geh_common.covernator_streamlit:main"
```

Run it with:

```sh
covernator [-o /path/to/save/files/to] [-p /path/to/look/for/scenario_tests] [-g] [-s]
```

Optional parameters are:

- -p / --path => this changes the folder to look for files (default `./tests`)
- -o / --output-dir => set the location where the files are being created that are used to run the streamlit app (default to a temporary folder)
- -g / --generate-only => used as a boolean flag. If provided, only files are created, but no streamlit app is running (does not make sence without defining the output_dir as the data will otherwise be lost)
- -s / --serve-only => used as a boolean flag. If provided, only runs the streamlit app without generating files (does not make sence without defining the output_dir as there won't be data to read from in a new temporary folder)

This will scan the folder defined in as path (default `./tests`) for scenario tests by searching for files with the name `coverage/all_cases*.yml` to find all cases that should be implemented and looks for all cases that are actually implemented in the `scenario_tests` folder on the same level as the `coverage` folder

### Folder Structure

Example of a valid folder structure:

```plaintext
├── scenario_group
    ├── coverage/
        ├── all_cases_scenario_group.yml
    └── scenario_tests/
        ├── given_something/
            └── coverage_mapping.yml
        └── given_another_thing/
            └── can_contain_multiple_layers/
                └── coverage_mapping.yml
```

- scenario group should have folder called `coverage` containing a yaml file following this pattern: `all_cases*.yml`
    - the `master file`
    - contains all cases that should be implemented
    - (in the future) boolean determines whether pipeline fails if case is not implemented
    - case names have to be unique in a single master file
    - Example: [all_cases_test.yml](./../../source/geh_common/tests/testing/unit/covernator/test_files/coverage/all_cases_test.yml)
- folder `scenario_tests` next to the coverage folder
    - must contain a key `cases_tested`
    - mapping between master file and scenarios testing specific cases
    - Examples: [first_layer_folder1/sub_folder/coverage_mapping.yml](./../../source/geh_common/tests/testing/unit/covernator/test_files/scenario_tests/first_layer_folder1/sub_folder/coverage_mapping.yml) & [first_layer_folder2/coverage_mapping.yml](./../../source/geh_common/tests/testing/unit/covernator/test_files/scenario_tests/first_layer_folder2/coverage_mapping.yml)

## Version 5.7.0

Workflows started to get the error:

- ImportError: cannot import name 'get_dist_dependency_conflicts' from 'opentelemetry.instrumentation.dependencies'

According to <https://github.com/Azure/azure-sdk-for-python/issues/40465> this was a breaking change introduced, but should be fixed in 1.6.7

## Version 5.6.6

**Subpackage**: `testing.delta_lake`

Fix bugs in functions `create_database()` and `create_table()`.

## Version 5.6.5

- `capacity_settlement_metering_point_periods_v1`
- `electrical_heating_child_metering_points_v1`
- `electrical_heating_consumption_metering_point_periods_v1`
- `missing_measurements_log_metering_point_periods_v1`
- `net_consumption_group_6_child_metering_points_v1`
- `net_consumption_group_6_consumption_metering_point_periods_v1`

## Version 5.6.4

Added data product electricity market contracts:

Bump to delta-spark>=3.3.0 dependency to ensure support for adding liquid clustering to
existing tables without clustering enabled.

With 3.3.0 clustering can be added like:

```sql
ALTER TABLE your_table_name CLUSTER BY column_name
```

## Version 5.6.3

- changed contract in measurements_core to be not nullable
- `measurements_core.measurements_gold.current_v1`

## Version 5.6.2

- Added argument to set the spark config error level.

## Version 5.6.1

Added data product contracts for:

- `measurements_calculated.calculated_measurements_v1`
- `measurements_calculated.missing_measurements_log_v1`

## Version 5.6.0

Adding new **Subpackage**: `geh_common.data_products`

Added the following data product:

- `measurements_gold.current_v1`

## Version 5.5.3

**Subpackages**: `geh_common.testing`

- Added parameter `extra_packages` for the spark session

## Version 5.5.2

- Instantiate LoggingSettings inside logging_configure

## Version 5.5.1

- Change the `start_job` method to allow the `python_params` argument to be optional.

## Version 5.5.0

**Subpackage**: `geh_common.domain.types`

Modifies enum `QuantityUnit` to map to the correct abbreviation values.

## Version 5.4.9

**Subpackage**: `geh_common.domain.types`

Extends enum `MeteringPointTypes` with one more type used in DH2.

## Version 5.4.8

**Subpackage**: `geh_common.testing`

Adds a utility function called `get_spark_test_session` that can be used to get a SparkSession for testing purposes.

Example usage:

```python
# In a function that uses Spark
from geh_common.testing import get_spark_test_session

def test_my_function():
    spark, _ = get_spark_test_session()
    assert my_function(spark).count() == 2

# As a fixture in a test file `conftest.py`
@pytest.fixture(scope="session")
def spark():
    session, data_dir = get_spark_test_session()
    yield session
    session.stop()
    shutil.rmtree(data_dir)

# As a fixture when pytest-xdist is enabled
# NOTE: When using pytest-xdist, the `-s` flag for pytest does not work.
# As a workaround, you can add the following fixture to your `conftest.py` file:
# @pytest.fixture(scope="session", autouse=True)
# def original_print():
#     """
#     pytest-xdist disables stdout capturing by default, which means that print() statements
#     are not captured and displayed in the terminal.
#     That's because xdist cannot support -s for technical reasons wrt the process execution mechanism
#     https://github.com/pytest-dev/pytest-xdist/issues/354
#     """
#     original_print = print
#     with pytest.MonkeyPatch.context() as m:
#         m.setattr(builtins, "print", lambda *args, **kwargs: original_print(*args, **{"file": sys.stderr, **kwargs}))
#         yield original_print
#         m.undo()
_session, data_dir = get_spark_test_session()

@pytest.fixture(scope="session")
def spark():
    yield _session
    _session.stop()
    shutil.rmtree(data_dir)
```

## Version 5.4.7

**Subpackage**: `geh_common.databricks`

- Updated error message to include state.

- Increased the default timeout value.

- Now wait_for_response it set to true by default.

## Version 5.4.6

**Subpackage**: `geh_common.domain.types`

Extends enum `OrchestrationTypes` with more types and descriptions.

## Version 5.4.5

**Subpackage**: `geh_common.testing`

Decorator `@testing` has been extended to support `DataFrameWrapper` and a selector function to
extract data frames from composite function results.

## Version 5.4.4

**Subpackage**: `geh_common.databricks`

- Updated the return type of databricks_api_client.execute_statement to StatementResponse, enabling the method to return data.

- Fixed a bug where invalid queries did not raise an exception.

- Extended databricks_api_client.execute_statement to be able to wait for response in the event that the warehouse needs
to start.

## Version 5.4.3

**Subpackage**: `geh_common.application`

Added new GridAreaCodes type with a validator

## Version 5.4.2

**Subpackage**: `geh_common.testing`

Fixes bug in decorator `@testing`, which prevented if from working when it was imported before
`configure_testing()` was executed.

## Version 5.4.1

- Removing `KILO_WATT_HOUR` enum as the `KWH` should be used instead.

## Version 5.4.0

**Subpackage**: `geh_common.testing`

Added decorator `@testing` to log content of data frames returned from function invocations.

Example usage:

```python
from geh_common.testing.dataframes import configure_testing, testing


@testing()
def my_function(spark: SparkSession) -> DataFrame:
    return spark.read.parquet("data.parquet")


if __name__ == "__main__":
    configure_testing(True)
    my_function()
```

Example output:

```text
>>>In some_module.py:91: my_function(...) returned:
+---+----+
|id |name|
+---+----+
|1  |a   |
|2  |b   |
+---+----+
```

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
