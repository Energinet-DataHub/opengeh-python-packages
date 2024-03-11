CREATE SCHEMA IF NOT EXISTS test_schema

GO

CREATE TABLE IF NOT EXISTS test_schema.test_table (column1 STRING, column2 STRING) USING delta

GO

INSERT INTO test_schema.test_table VALUES ('test1', 'test1')

GO
