# Testcommon Release Notes

## Version 0.0.1

Added `TestCases` that, if subclassed, can discover files where feature tests are defined. This is related to deprecating the `Covernator`, which is a PowerShell + Excel-based test discovery tool.

Added utility functions to support ETL (Extract-Transform-Load) testing. In this implementation ETL testing means testing a transformation based on input as CSV files and expected output stated as CSV files.
The intended use case is Spark jobs.
