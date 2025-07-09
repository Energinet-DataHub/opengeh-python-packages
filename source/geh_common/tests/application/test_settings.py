import sys

import pytest
from pydantic import Field

from geh_common.application.settings import ApplicationSettings


class MyArgs(ApplicationSettings):
    my_mandatory_arg: str = Field(init=False)
    my_optional_arg: str | None = None


def test_when_optional_arg_not_present__returns_expected_arg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrrange
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--my-mandatory-arg", "some_value"])

    # Act
    args = MyArgs()

    # Assert
    assert args.my_mandatory_arg == "some_value"
    assert args.my_optional_arg is None


def test_when_optional_arg_is_present__returns_expected_arg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrrange
    monkeypatch.setattr(
        sys,
        "argv",
        ["dummy_script_name", "--my-mandatory-arg", "some_value", "--my-optional-arg", "some_optional_value"],
    )

    # Act
    args = MyArgs()

    # Assert
    assert args.my_mandatory_arg == "some_value"
    assert args.my_optional_arg == "some_optional_value"


def test_when_mandatory_arg_not_present__raises_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--my-optional-arg", "some_optional_value"])

    # Act & Assert
    with pytest.raises(ValueError, match="Field required"):
        MyArgs()  # Should raise an error because my_mandatory_arg is not provided


def test_when_default_settings_and_unknown_argument__raises_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setattr(
        sys, "argv", ["dummy_script_name", "--my-mandatory-arg", "some_value", "--unknown-arg", "some_unknown_value"]
    )

    # Act
    with pytest.raises(SystemExit) as excinfo:
        MyArgs()

    # Assert
    assert excinfo.value.code == 2  # SystemExit code for unknown arguments


def test_when_ignore_unknown_argument__ignores_unknown_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    class MyArgsWithIgnoreUnknown(ApplicationSettings, cli_ignore_unknown_args=True):
        my_mandatory_arg: str = Field(init=False)
        my_optional_arg: str | None = None

    monkeypatch.setattr(
        sys, "argv", ["dummy_script_name", "--my-mandatory-arg", "some_value", "--unknown-arg", "some_unknown_value"]
    )

    # Act
    args = MyArgsWithIgnoreUnknown()

    # Assert
    assert args.my_mandatory_arg == "some_value"
