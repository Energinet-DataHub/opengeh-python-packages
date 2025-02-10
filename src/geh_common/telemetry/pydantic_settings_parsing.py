from pydantic_settings import BaseSettings


class PydanticParsingSettings(
    BaseSettings,
    cli_parse_args=True,
    cli_kebab_case=True,
    cli_ignore_unknown_args=True,
    cli_implicit_flags=True,
):
    """Base class for application settings.

    Supports:
    - CLI parsing with arguments using kebab-case.
    - Environment variables using SNAKE_UPPER_CASE.
    - Ignoring unknown CLI arguments. This behavior can be overridden by setting `cli_ignore_unknown_args=False`
      in the class definition of the derived settings class. Example:
      `class LoggingSettings(ApplicationSettings, cli_ignore_unknown_args=False):`
    """

    pass
