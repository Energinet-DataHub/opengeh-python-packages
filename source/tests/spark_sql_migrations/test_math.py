from spark_sql_migrations.add.add import add


def test_add() -> None:
    # Arrange
    number1 = 1
    number2 = 2
    expected = 3

    # Act
    actual = add(number1, number2)

    # Assert
    assert actual == expected
