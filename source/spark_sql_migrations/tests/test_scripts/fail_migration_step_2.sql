ALTER TABLE spark_catalog.test_schema.test_table_fail
ADD COLUMNS (column2 STRING)

GO

ALTER TABLE spark_catalog.test_schema.test_table_fail
ALTER COLUMN quantity DECIMAL(18,3)

GO
