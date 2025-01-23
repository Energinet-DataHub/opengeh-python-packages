import os
import sys
from typing import Any, Iterator, Tuple, Type, Optional
import uuid

from pydantic_settings import (
    BaseSettings,
    CliSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict
)


class LoggingSettings(BaseSettings):
    """
    LoggingSettings class uses Pydantic BaseSettings to configure and validate parameters.
    Parameters can come from both runtime (CLI) or from environment variables.
    The priority is CLI parameters first and then environment variables.
    """
    cloud_role_name: str
    applicationinsights_connection_string: str = None
    subsystem: str
    orchestration_instance_id: Optional[uuid.UUID] = None
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


INTEGRATION_TEST_LOGGER_NAME = "test-logger"
INTEGRATION_TEST_CLOUD_ROLE_NAME = "test-cloud-role-name"
INTEGRATION_TEST_TRACER_NAME = "test-tracer-name"
os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = "connectionString"
os.environ['CLOUD_ROLE_NAME'] = INTEGRATION_TEST_CLOUD_ROLE_NAME
os.environ['SUBSYSTEM'] = INTEGRATION_TEST_TRACER_NAME
os.environ['ORCHESTRATION_INSTANCE_ID'] = str(uuid.uuid4())
logging_settings = LoggingSettings()

cast = str(logging_settings.orchestration_instance_id)
testDictionary = {"key": "value"}
testDictionary = testDictionary | {"orchestration_instance_id": str(logging_settings.orchestration_instance_id)}

print("debug line")
