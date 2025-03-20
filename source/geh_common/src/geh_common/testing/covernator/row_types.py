from dataclasses import dataclass
from typing import List


@dataclass
class CaseRow:
    path: str
    case: str
    implemented: bool


@dataclass
class ScenarioRow:
    source: str
    cases_tested: List[str]
