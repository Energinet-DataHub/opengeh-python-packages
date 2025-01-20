import os
import sys
from typing import Tuple, Type, Any

from pydantic_settings import (
    BaseSettings,
    CliSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict
)

print(sys.argv[1:])

class Settings(BaseSettings):
    # model_config = SettingsConfigDict(cli_parse_args=True)
    cli_foo1: str
    # cli_foo2: str
    # env_foo1: str
    # env_foo2: str
    input_foo1: str = "Hello"
    force_configuration: bool = False

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return CliSettingsSource(settings_cls, cli_parse_args=True, cli_ignore_unknown_args=True), env_settings

os.environ['ENV_FOO1'] = 'foo1 from environment'
os.environ['ENV_FOO2'] = 'foo2 from environment'

settings = Settings()

print(settings)



#
# settingsOuter = 1
#
# # sys.argv = ['example.py', '--my_foo=from cli']
# try:
#     settings = Settings()
#     settingsOuter = settings
# except Exception as e:
#     print("additional parameters were added but not expected")
# #settings.test_field = 'goodbye'
#
# print(settingsOuter)
# #print(Settings().model_dump_json())
# #> {'my_foo': 'from environment'}
