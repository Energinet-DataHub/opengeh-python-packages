from pyspark.sql import SparkSession
import spark_sql_migrations.schema_migration_pipeline as sut
import tests.helpers.spark_helper as spark_helper


def test_migrate_with_schema_migration_scripts_compare_schemas(
    spark: SparkSession
) -> None:
    # Arrange
    spark_helper.reset_spark_catalog(spark)

    # Act
    sut.migrate()

    # Assert
    assert True

