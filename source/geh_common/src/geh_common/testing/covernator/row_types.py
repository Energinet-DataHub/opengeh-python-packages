from dataclasses import dataclass, field
from typing import List


@dataclass
class ScenarioRow:
    source: str
    cases_tested: List[str]


@dataclass
class CaseRow:
    path: str
    case: str
    implemented: bool
    scenarios: List[ScenarioRow] = field(default_factory=list)
