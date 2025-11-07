"""Covernator package initialization.

Provides unified exports for high-level command functions and model dataclasses,
replacing the legacy CaseRow/ScenarioRow definitions.
"""

from geh_common.testing.covernator.commands import (
    find_all_cases,
    find_all_scenarios,
    run_covernator,
)
from geh_common.testing.covernator.models import (
    CaseInfo,
    CoverageMapping,
    CovernatorResults,
)

__all__ = [
    "run_covernator",
    "find_all_cases",
    "find_all_scenarios",
    "CaseInfo",
    "CoverageMapping",
    "CovernatorResults",
]
