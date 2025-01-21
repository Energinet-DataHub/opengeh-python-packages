import os
import sys
import pytest
import unittest.mock as mock
from telemetry_logging.logging_configuration import (
    configure_logging,
    get_extras,
    add_extras,
    get_tracer,
    start_span,
    _IS_INSTRUMENTED,
    LoggingSettings
)
from uuid import UUID
from pydantic import ValidationError

print(os.environ.get('CLOUD_ROLE_NAME'))
# assert os.environ['CLOUD_ROLE_NAME'] != 'cloud_role_name'
