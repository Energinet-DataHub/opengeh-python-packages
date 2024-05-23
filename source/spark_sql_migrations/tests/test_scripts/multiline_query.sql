CREATE SCHEMA IF NOT EXISTS spark_catalog.test_schema

GO

CREATE TABLE IF NOT EXISTS spark_catalog.test_schema.test_table (column1 STRING, column2 STRING) USING delta

GO

ALTER TABLE spark_catalog.test_schema.test_table
DROP CONSTRAINT IF EXISTS column1

GO
