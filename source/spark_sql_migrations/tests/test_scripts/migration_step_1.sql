CREATE SCHEMA IF NOT EXISTS spark_catalog.test_schema

GO

CREATE TABLE IF NOT EXISTS spark_catalog.test_schema.test_table (column1 STRING, column2 STRING) USING delta

GO

INSERT INTO spark_catalog.test_schema.test_table VALUES ('test1', 'test1')

GO
