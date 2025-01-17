import os
import sys
from typing import Tuple, Type, Optional
from uuid import UUID

from pydantic_settings import (
    BaseSettings,
    CliSettingsSource,
    PydanticBaseSettingsSource
)




class LoggingSettings(BaseSettings):
    """
    LoggingSettings class uses Pydantic BaseSettings to configure and validate parameters.
    Parameters can come from both runtime (CLI) or from environment variables.
    The priority is CLI parameters first and then environment variables.
    """
    cloud_role_name: str
    applicationinsights_connection_string: str
    subsystem: str
    orchestration_instance_id: Optional[UUID] = None


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



os.environ['CLOUD_ROLE_NAME'] = 'cloud_role_name from environment'
os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = 'applicationinsights_connection_string from environment'
os.environ['SUBSYSTEM'] = 'subsystem from environment'


settings = LoggingSettings()
# settings.input_foo1 = "Goodbye World"
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
