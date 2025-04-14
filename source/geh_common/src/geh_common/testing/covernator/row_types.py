from dataclasses import dataclass
from typing import List


@dataclass
class ScenarioRow:
    source: str
    cases_tested: List[str]
    group: str | None = None


@dataclass
class CaseRow:
    path: str
    case: str
    implemented: bool
    group: str | None = None
