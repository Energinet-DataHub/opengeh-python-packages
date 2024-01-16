from common.add import add


def test_add() -> None:
    # Arrange
    number1 = 1
    number2 = 2

    # Act
    result = add(number1, number2)

    # Assert
    assert result == 3
