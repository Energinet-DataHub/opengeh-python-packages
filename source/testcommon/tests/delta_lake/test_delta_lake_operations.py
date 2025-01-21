from pyspark.sql import SparkSession

from source.testcommon.testcommon.delta_lake.delta_lake_operations import create_database


def test_create_database__creates_database(spark: SparkSession):
    # Arrange
    database_name = "test_db"

    # Act
    create_database(spark, database_name)

    # Assert
    databases = [db.name for db in spark.catalog.listDatabases()]
    assert database_name in databases

def test_create_database__when_already_exists__does_not_create(spark: SparkSession):
    # Arrange
    database_name = "existing_db"
    create_database(spark, database_name)  # Create the database initially

    # Act
    create_database(spark, database_name)  # Try to create the same database again

    # Assert
    databases = [db.name for db in spark.catalog.listDatabases()]
    assert databases.count(database_name) == 1  # Ensure the database is not duplicated
