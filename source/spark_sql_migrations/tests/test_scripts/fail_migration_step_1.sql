CREATE SCHEMA IF NOT EXISTS test_schema

GO

CREATE TABLE IF NOT EXISTS test_schema.test_table_fail (column1 STRING, quantity DECIMAL(18,6)) USING delta

GO

INSERT INTO test_schema.test_table_fail VALUES ('test1', 10.123456)

GO
