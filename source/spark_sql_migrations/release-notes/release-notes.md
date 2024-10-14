# Spark SQL Migrations Release Notes

## Version 1.10.0

- Added `rollback_on_failure` parameter to the `SparkSqlMigrationsConfiguration` class. This parameter allows the user to specify whether the migration should be rolled back if an error occurs during the migration process. The default value is `False`.

### Changes

As the behavior before was `True`, the default value is now `False`. This means that if an error occurs during the migration process, the migration will not be rolled back by default. If the user wants to roll back the migration in case of an error, they can set the `rollback_on_failure` parameter to `True`.

Example:

``` python
spark_config = SparkSqlMigrationsConfiguration(
    ...
    rollback_on_failure=True
 )
```

## Version 1.9.2

- Adding release notes

## Version 1.9.1

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
