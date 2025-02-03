"""To configure the logging module use the submodule `logging_configuration.py`.

The following are the generally available hooks for logging from code.
"""

from opengeh_common.telemetry.decorators import use_span
from opengeh_common.telemetry.logger import Logger
from opengeh_common.telemetry.span_recording import span_record_exception

__all__ = [
    Logger.__name__,
    use_span.__name__,
    span_record_exception.__name__,
]  # type: ignore
