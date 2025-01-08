# Testcommon Release Notes

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

Added `TestCases` that, if subclassed, can discover files where feature tests are defined. This is related to deprecating the `Covernator`, which is a PowerShell + Excel-based test discovery tool.

Added utility functions to support ETL (Extract-Transform-Load) testing. Here ETL testing means testing a transformation based on input as CSV files and expected output stated as CSV files.
The intended use case is Spark jobs.
