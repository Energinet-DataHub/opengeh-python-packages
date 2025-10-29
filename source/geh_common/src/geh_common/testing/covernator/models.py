from dataclasses import dataclass
from typing import List


@dataclass
class CoverageStats:
    total_cases: int
    total_scenarios: int
    total_groups: int


@dataclass
class LogEntry:
    timestamp: str
    message: str


@dataclass
class CaseInfo:
    group: str
    path: str
    case: str
    implemented: bool


@dataclass
class CoverageMapping:
    group: str
    case: str
    scenario: str


@dataclass
class CovernatorResults:
    stats: CoverageStats
    info_logs: List[LogEntry]
    error_logs: List[LogEntry]
    all_cases: List[CaseInfo]
    coverage_map: List[CoverageMapping]
