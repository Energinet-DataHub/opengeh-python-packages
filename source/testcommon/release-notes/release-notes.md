# Testcommon Release Notes

## Version 0.0.3

Include test file extension in function `get_then_names` to adhere to functionality of class `TestCase`.

## Version 0.0.2

Function `get_then_names` of `Testcommon.etl` is updated to include subfolders to avoid naming conflicts.

## Version 0.0.1

Added `TestCases` that, if subclassed, can discover files where feature tests are defined. This is related to deprecating the `Covernator`, which is a PowerShell + Excel-based test discovery tool.

Added utility functions to support ETL (Extract-Transform-Load) testing. Here ETL testing means testing a transformation based on input as CSV files and expected output stated as CSV files.
The intended use case is Spark jobs.
