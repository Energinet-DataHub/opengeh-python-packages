import spark_sql_migrations.migrations.schema_migration_pipeline as sut


def test_migrate() -> None:
    # Arrange
    # spark_helper.reset_spark_catalog(spark)

    # Act
    sut.migrate()

    # Assert
    assert True
