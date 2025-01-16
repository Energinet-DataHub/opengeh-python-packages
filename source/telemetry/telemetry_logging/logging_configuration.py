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
from typing import Any, Iterator
from pydantic import BaseModel, Field, ValidationError
#from pydantic_settings import BaseSettings
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

class CLIArgs(BaseModel):
    """
    A class used to hold CLI arguments passed to the application
    Add new fields to the CLIArgs class as needed
    """
    orchestration_instance_id: UUID = Field(..., description="The UUID of the orchestration instance")

    @classmethod
    def parse_from_cli(cls):
        # Initialize argparse
        parser = argparse.ArgumentParser(description="Process the orchestration instance UUID.")
        parser.add_argument('--orchestration_instance_id', type=str, required=True, help='Your name')

        # Parse the arguments
        args = parser.parse_args()

        try:
            # Use Pydantic to validate and parse the UUID input
            return cls(orchestration_instance_id=args.orchestration_instance_id)

        except ValidationError as e:
            print(f"Validation failed:\n{e.json()}")

# class ENVArgs(BaseSettings):
#     """
#     A class used to hold environment variables passed to the application
#     Add new fields to the ENVArgs class as needed
#     """
#     cloud_role_name: str = Field(validation_alias="CLOUD_ROLE_NAME")
#     applicationinsights_connection_string: str = Field(validation_alias="APPLICATIONINSIGHTS_CONNECTION_STRING")
#     subsystem: str = Field(validation_alias="SUBSYSTEM")
#
# @dataclass
# class LoggingSettings(BaseSettings):
#     """Logging settings class used to configure logging for the provided app"""
#     cloud_role_name: str
#     subsystem: str
#     applicationinsights_connection_string: str = None # If set to null, logging will not be sent to Azure Monitor
#     logging_extras: dict = None # Custom structured logging data to be included in every log message.
#
#     @classmethod
#     def load(cls):
#         # Load CLI args
#         cli_args = CLIArgs.parse_from_cli()
#
#         # Load environment args
#         env_args = ENVArgs()
#
#         # Combine and return LoggingSettings
#         return cls(
#             cloud_role_name=env_args.cloud_role_name,
#             applicationinsights_connection_string=env_args.applicationinsights_connection_string,
#             subsystem=env_args.subsystem,
#             logging_extras = {"OrchestrationInstanceId": cli_args.orchestration_instance_id}
#         )

def configure_logging(
    *,
    cloud_role_name: str,
    tracer_name: str,
    applicationinsights_connection_string: str | None = None,
    extras: dict[str, Any] | None = None,
    force_configuration: bool = False,
) -> None:
    """
    Configure logging to use OpenTelemetry and Azure Monitor.

    :param cloud_role_name:
    :param tracer_name:
    :param applicationinsights_connection_string:
    :param extras: Custom structured logging data to be included in every log message.
    :param force_configuration: If True, then logging will be reconfigured even if it has already been configured.
    :return:

    If connection string is None, then logging will not be sent to Azure Monitor.
    This is useful for unit testing.
    """

    global _TRACER_NAME
    _TRACER_NAME = tracer_name

    # Only configure logging once unless forced.
    global _IS_INSTRUMENTED
    if _IS_INSTRUMENTED and not force_configuration:
        return

    # Configure structured logging data to be included in every log message.
    if extras is not None:
        global _EXTRAS
        _EXTRAS = extras.copy()

    # Add cloud role name when logging
    os.environ["OTEL_SERVICE_NAME"] = cloud_role_name

    # Configure OpenTelemetry to log to Azure Monitor.
    if applicationinsights_connection_string is not None:
        configure_azure_monitor(connection_string=applicationinsights_connection_string)
        _IS_INSTRUMENTED = True

    # Reduce Py4J logging. py4j logs a lot of information.
    logging.getLogger("py4j").setLevel(logging.WARNING)

    # Mark logging state as configured
    global _LOGGING_CONFIGURED
    _LOGGING_CONFIGURED = True

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
