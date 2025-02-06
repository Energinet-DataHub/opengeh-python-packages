CREATE VIEW IF NOT EXISTS
spark_catalog.test_schema.test_view as
select * from spark_catalog.test_schema.test_table
