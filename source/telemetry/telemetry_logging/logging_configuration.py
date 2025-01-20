# Copyright 2020 Energinet DataHub A/S
#
# Licensed under the Apache License, Version 2.0 (the "License2");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import logging
import os
import argparse
from typing import Any, Iterator, Tuple, Type, Optional
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings, CliSettingsSource, PydanticBaseSettingsSource
from uuid import UUID
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.trace import Span, Tracer
from dataclasses import dataclass

DEFAULT_LOG_FORMAT: str = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DEFAULT_LOG_LEVEL: int = logging.INFO
_EXTRAS: dict[str, Any] = {}
_IS_INSTRUMENTED: bool = False
_TRACER: Tracer | None = None
_TRACER_NAME: str
_LOGGING_CONFIGURED: bool = False # Flag to track if logging is configured

def set_logging_configured(configured: bool) -> None:
    """Sets the global flag indicating logging has been configured."""
    global _LOGGING_CONFIGURED
    _LOGGING_CONFIGURED = configured

def get_logging_configured() -> bool:
    """Returns the current logging configuration state."""
    return _LOGGING_CONFIGURED

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


def configure_logging(
    *,
    logging_settings: LoggingSettings,
    extras: dict[str, Any],
) -> None:
    """
    Configure logging to use OpenTelemetry and Azure Monitor.
    :param logging_settings: Logging settings object
    :param extras: Custom structured logging data to be included in every log message.
    :return:
    If connection string is None, then logging will not be sent to Azure Monitor.
    This is useful for unit testing.
    """

    global _TRACER_NAME
    _TRACER_NAME = logging_settings.subsystem

    # Only configure logging once unless forced.
    global _IS_INSTRUMENTED
    if _IS_INSTRUMENTED and not logging_settings.force_configuration:
        return

    # Configure structured logging data to be included in every log message.
    if extras is not None:
        global _EXTRAS
        _EXTRAS = extras.copy()

    # Add cloud role name when logging
    os.environ["OTEL_SERVICE_NAME"] = logging_settings.cloud_role_name

    # Configure OpenTelemetry to log to Azure Monitor.
    if logging_settings.applicationinsights_connection_string is not None:
        configure_azure_monitor(connection_string=logging_settings.applicationinsights_connection_string)
        _IS_INSTRUMENTED = True

    # Reduce Py4J logging. py4j logs a lot of information.
    logging.getLogger("py4j").setLevel(logging.WARNING)

    # Mark logging state as configured
    global _LOGGING_CONFIGURED
    _LOGGING_CONFIGURED = True

    # Adding orchestration ID as an extra when provided through LoggingSettings:
    add_extras({"orchestration_instance_id": logging_settings.orchestration_instance_id})


def get_extras() -> dict[str, Any]:
    return _EXTRAS.copy()


def add_extras(extras: dict[str, Any]) -> None:
    global _EXTRAS
    _EXTRAS = _EXTRAS | extras


def get_tracer() -> Tracer:
    global _TRACER
    if _TRACER is None:
        global _TRACER_NAME
        _TRACER = trace.get_tracer(_TRACER_NAME)
    return _TRACER


@contextlib.contextmanager
def start_span(name: str) -> Iterator[Span]:
    with get_tracer().start_as_current_span(name, attributes=get_extras()) as span:
        yield span
