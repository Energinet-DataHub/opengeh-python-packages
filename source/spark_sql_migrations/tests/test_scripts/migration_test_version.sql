CREATE SCHEMA IF NOT EXISTS test_schema

GO

-- 'categorycolumn' contains the "go" keyword.
CREATE TABLE IF NOT EXISTS test_schema.test_table (column1 STRING, categorycolumn STRING) USING delta

GO

ALTER TABLE test_schema.test_table
ADD COLUMNS (column2 STRING)

GO
