CREATE SCHEMA spark_catalog.test_schema;

GO

CREATE TABLE IF NOT EXISTS spark_catalog.test_schema.test_table (column1 STRING, column2 STRING) USING delta

GO

CREATE TABLE IF NOT EXISTS spark_catalog.test_schema.test_table_2 (column1 STRING, column2 STRING) USING delta
