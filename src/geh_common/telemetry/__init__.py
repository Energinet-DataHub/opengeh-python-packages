"""To configure the logging module use the submodule `logging_configuration.py`.

The following are the generally available hooks for logging from code.
"""

from geh_common.telemetry.decorators import use_span
from geh_common.telemetry.logger import Logger
from geh_common.telemetry.span_recording import span_record_exception

__all__ = [
    "Logger",
    "use_span",
    "span_record_exception",
]
