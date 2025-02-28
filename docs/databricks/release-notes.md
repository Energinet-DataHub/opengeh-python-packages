# Databricks Release Notes

## Version 5.3.0

*Breaking Changes*

- Moved `databricks_api_client` from `testing.container_test` to `databricks`.
- Renamed `seed` method in `databricks_api_client` to `execute_statement`.
- Removed `catalog` and `schema` from `execute_statement` method. This can be set in the statement if neccessary.
