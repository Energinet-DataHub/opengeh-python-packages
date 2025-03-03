# Databricks Release Notes

## Version 5.3.1

- Add `cancel_job_run`. The function waits for the job run to be cancelled by default.
- Add `get_job_run_id` to get the first run id for a Databricks job.

## Version 5.3.0

### Breaking Changes

- Moved `databricks_api_client` from `testing.container_test` to `databricks`.
- Renamed `seed` method in `databricks_api_client` to `execute_statement`.
- Removed `catalog` and `schema` from `execute_statement` method. This can be set in the statement if neccessary.
