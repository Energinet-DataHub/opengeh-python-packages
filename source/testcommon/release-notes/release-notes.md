# Testcommon Release Notes

## Version 0.3.1

Added test for `write_to_delta` and the added functionality to `get_then_names`

## Version 0.3.0

Renamed testcommon.etl to testcommon.scenario_testing to provide a clearer indication of its intended use.

## Version 0.2.1

Added scenario testing documentation

## Version 0.2.0

Added container test functionality with DatabricksApiClient.

## Version 0.1.1

Added `write_to_delta` to init.py.

## Version 0.1.0

Modified `get_then_names` to accept a `scenario_path` as a parameter for locating the `/then` files.
Added a `write_to_delta.py` file for writing a list of test CSV files to a delta table.

## Version 0.0.9

Add `__init__.py` in `delta_lake` folder

## Version 0.0.8

Added support for creating tables and databases in Delta Lake (`create_database` and `create_table`).

## Version 0.0.7

Nullability is now handled correctly when parsing CSV files to dataframes.
Before this change, all columns were treated as nullable.

## Version 0.0.6

Support for enabling or disabling 'ignoring columns' in the dataframe assert function.

Add test setting strict_actual_assertion to enable or disable strict assertion of dataframes.
When true cells with 'ignored_value' will not be ignored.

## Version 0.0.5

Add support for ignoring columns in the `read_csv` function.

Some columns are not easily replicated in testing, such as timestamps or other columns that are not deterministic.
These columns can now be ignored by setting their rows to the `ignored_value` parameter in the `read_csv` function.

## Version 0.0.4

Support custom function `assert_dataframes_and_schemas` for more configuration options.

As a sideeffect the heavy dependencies on pandas and more have been removed. This
greatly improves performance of building docker images depending on this package.

## Version 0.0.3

Add test to verify that the parts of `testcommon.etl` work together.
See the test in the test `test_etl` in the test module `test_etl.py`.
This test also provides a simple example of how to write tests using this framework.

## Version 0.0.2

Function `get_then_names` of `Testcommon.etl` is updated to include subfolders to avoid naming conflicts.

## Version 0.0.1

Added `TestCases` that, if subclassed, can discover files where feature tests are defined. This is related to
deprecating the `Covernator`, which is a PowerShell + Excel-based test discovery tool.

Added utility functions to support ETL (Extract-Transform-Load) testing. Here ETL testing means testing a transformation
based on input as CSV files and expected output stated as CSV files.
The intended use case is Spark jobs.
