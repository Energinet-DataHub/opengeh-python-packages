import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import polars as pl
import yaml
from enum import Enum, auto


# =====================================================================
# === Enums & Dataclasses =============================================
# =====================================================================
class LogLevel(Enum):
    INFO = auto()
    ERROR = auto()


@dataclass
class LogEntry:
    timestamp: str
    level: LogLevel
    message: str


@dataclass
class RunStats:
    total_cases: int = 0
    total_scenarios: int = 0
    total_groups: int = 0
    generated_at: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# =====================================================================
# === DuplicateKey YAML Loader =======================================
# =====================================================================
class DuplicateKeyLoader(yaml.SafeLoader):
    """
    YAML loader that detects and reports duplicate keys.
    Logs structured errors to OutputManager if provided.
    """

    def __init__(self, stream, logger=None, group=None, scenario=None):
        super().__init__(stream)
        self.logger = logger
        self.group = group
        self.scenario = scenario

    def construct_mapping(self, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping and self.logger:
                context = f"[{self.group}]"
                if self.scenario:
                    context += f"[{self.scenario}]"
                self.logger.log(
                    f"{context} Duplicate key in YAML: {key}",
                    level=LogLevel.ERROR,
                )
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


# =====================================================================
# === OutputManager: Unified CI Output, Logging & Artifacts ===========
# =====================================================================
class OutputManager:
    """
    Centralized output manager for Covernator and CI coverage validation.
    Handles:
      - Console + JSON logging
      - Structured CSV/JSON artifacts
      - Duplicate-aware log collection
      - CI strict failure behavior
    """

    def __init__(self, output_dir: Path, strict: bool = True):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.strict = strict

        self.logs: List[LogEntry] = []
        self.logged_messages: set[str] = set()

        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            handlers=[logging.StreamHandler()],
        )

    # ==============================================================
    # === Logging Helpers ==========================================
    # ==============================================================
    def log(self, message: str, level: LogLevel = LogLevel.INFO):
        """Log message to console and memory with timestamp."""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level_tag = "[INFO]" if level == LogLevel.INFO else "[ERROR]"
        formatted = f"[{ts}] {level_tag} {message}"

        # console output
        if level == LogLevel.ERROR:
            logging.error(formatted)
        else:
            logging.info(formatted)

        # store log
        self.logs.append(LogEntry(ts, level, f"{level_tag} {message}"))

        # enforce CI strict mode
        if self.strict and level == LogLevel.ERROR:
            self._set_failure_flag()

    def log_once(self, message: str, level: LogLevel = LogLevel.INFO):
        """Avoid duplicate log entries for repeated messages."""
        if message not in self.logged_messages:
            self.logged_messages.add(message)
            self.log(message, level)

    def _set_failure_flag(self):
        """Mark pipeline as failed via environment or state."""
        self.pipeline_failed = True

    # ==============================================================
    # === YAML Loader ==============================================
    # ==============================================================
    def load_yaml(self, path: Path, group=None, scenario=None) -> Optional[dict]:
        """Load YAML safely with duplicate detection."""
        if not path.exists():
            self.log(f"Missing YAML file: {path}", level=LogLevel.ERROR)
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                loader = lambda stream: DuplicateKeyLoader(
                    stream, logger=self, group=group, scenario=scenario
                )
                return yaml.load(f, Loader=loader)
        except yaml.YAMLError as e:
            self.log(f"YAML parse error in {path}: {e}", level=LogLevel.ERROR)
            if self.strict:
                raise
        except Exception as e:
            self.log(f"Unexpected error reading {path}: {e}", level=LogLevel.ERROR)
            if self.strict:
                raise
        return None

    # ==============================================================
    # === Artifact Writers =========================================
    # ==============================================================
    def write_csv(self, name: str, df: pl.DataFrame):
        """Persist DataFrame to CSV within output directory."""
        csv_path = self.output_dir / name
        df.write_csv(csv_path)
        self.log(f"Wrote CSV artifact: {csv_path}", level=LogLevel.INFO)

    def write_json(self, name: str, data: dict):
        """Write JSON file with indentation and UTF-8 encoding."""
        json_path = self.output_dir / name
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        self.log(f"Wrote JSON artifact: {json_path}", level=LogLevel.INFO)

    # ==============================================================
    # === Finalization =============================================
    # ==============================================================
    def finalize(self, stats: RunStats):
        """Save logs and stats to a single stats.json artifact."""
        payload = {
            **asdict(stats),
            "logs": [asdict(log) for log in self.logs],
        }
        self.write_json("stats.json", payload)

        # fail fast if needed
        if getattr(self, "pipeline_failed", False) and self.strict:
            self.log("❌ CI pipeline failed due to logged errors.", level=LogLevel.ERROR)
            raise SystemExit(1)
        else:
            self.log("✅ CI pipeline completed successfully.", level=LogLevel.INFO)
