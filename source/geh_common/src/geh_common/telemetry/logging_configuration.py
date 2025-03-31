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
from typing import Any, Iterator
from uuid import UUID

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.trace import Span, Tracer
from pydantic import Field

from geh_common.application.settings import ApplicationSettings

DEFAULT_LOG_FORMAT: str = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DEFAULT_LOG_LEVEL: int = logging.INFO
_EXTRAS: dict[str, Any] = {}
_IS_INSTRUMENTED: bool = False
_TRACER: Tracer | None = None
_TRACER_NAME: str


def set_extras(extras: dict[str, Any]) -> None:
    """Set the extras dictionary."""
    global _EXTRAS
    _EXTRAS = extras


def get_is_instrumented() -> bool:
    """Return the current logging configuration state."""
    return _IS_INSTRUMENTED


def set_is_instrumented(is_instrumented: bool) -> None:
    """Set the instrumentation state."""
    global _IS_INSTRUMENTED
    _IS_INSTRUMENTED = is_instrumented


def set_tracer(tracer: Tracer | None) -> None:
    """Set the tracer instance."""
    global _TRACER
    _TRACER = tracer


def set_tracer_name(tracer_name: str) -> None:
    """Set the tracer name."""
    global _TRACER_NAME
    _TRACER_NAME = tracer_name


class LoggingSettings(ApplicationSettings):
    """Configuration settings for logging, including OpenTelemetry and Azure Monitor integration.

    This class extends `ApplicationSettings` to define and validate the necessary logging parameters.
    It can be instantiated without explicitly passing arguments, provided that the required settings
    are available via environment variables or CLI arguments.

    Attributes:
        cloud_role_name (str): The role name used for cloud-based logging. Please use a cloud_role_name that is unique for the job being executed. This will be searchable in Applicaiton Insights as the "Role Name"
        applicationinsights_connection_string (str): The connection string for Azure Application Insights.
        subsystem (str): The name of the subsystem or application component. This can be used to search for events in Application Insights using "Select property" -> "Subsystem".
        orchestration_instance_id (UUID | None): A unique identifier for the orchestration instance. Also searchable under Properties in Application Insights if added.

    Example:
        ```python
        import os
        from logging_config import LoggingSettings, configure_logging

        # Ensure required environment variables are set
        os.environ["CLOUD_ROLE_NAME"] = "MyService"
        os.environ["SUBSYSTEM"] = "Measurements"

        # A parameter to the execution could be passed like this:
        # --orchestration-instance-id = "123e4567-e89b-12d3-a456-426614174000"

        # Instantiate settings (automatically pulls from env vars)
        logging_settings = LoggingSettings(tracer_name="tracer_name_for_job_name")

        # Configure logging with the settings
        configure_logging(logging_settings=logging_settings)
        ```
    """

    cloud_role_name: str = Field()
    subsystem: str = Field()
    orchestration_instance_id: UUID | None = Field(default=None)
    applicationinsights_connection_string: str = Field(init=False, repr=False)


def configure_logging(
    *,
    cloud_role_name: str,
    subsystem: str,
    extras: dict[str, Any] | None = None,
) -> LoggingSettings:
    """Configure logging to use OpenTelemetry and Azure Monitor.

    Must have applicationinsights_connection_string defined as an environment variable.

    Args:
        cloud_role_name: The logging settings object.
        subsystem: Name of the subsystem.
        extras: Custom structured logging data to be included in every log message.

    Returns:
        LoggingSettings: The configured logging settings object.

    """
    global _TRACER_NAME

    logging_settings = LoggingSettings(cloud_role_name=cloud_role_name, subsystem=subsystem)

    _TRACER_NAME = logging_settings.cloud_role_name

    # Only configure logging if not already instrumented
    if get_is_instrumented():
        return

    # Configure structured logging data to be included in every log message.
    if extras is not None:
        global _EXTRAS
        _EXTRAS = extras.copy()

    # Add cloud role name when logging
    os.environ["OTEL_SERVICE_NAME"] = logging_settings.cloud_role_name

    # Configure OpenTelemetry to log to Azure Monitor.
    configure_azure_monitor(connection_string=logging_settings.applicationinsights_connection_string)

    # Reduce Py4J logging. py4j logs a lot of information.
    logging.getLogger("py4j").setLevel(logging.WARNING)

    # Add extras to log messages
    if logging_settings.orchestration_instance_id is not None:
        add_extras({"orchestration_instance_id": str(logging_settings.orchestration_instance_id)})
    add_extras({"Subsystem": logging_settings.subsystem})

    # Mark logging state as configured
    set_is_instrumented(True)

    return logging_settings


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
