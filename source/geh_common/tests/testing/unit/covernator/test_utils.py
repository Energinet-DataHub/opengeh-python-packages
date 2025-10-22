import json
from pathlib import Path
from geh_common.testing.covernator.commands import run_covernator


def run_and_load_stats(base_path: Path, tmp_path: Path) -> tuple[Path, dict]:
    """
    Runs the covernator process and loads the resulting stats.json file.

    Returns:
        - output_dir (Path): Path to output directory (for checking generated CSVs)
        - stats (dict): Parsed stats.json containing total_cases, total_scenarios, etc.
    """
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    run_covernator(output_dir, base_path)

    stats_path = output_dir / "stats.json"
    stats = json.loads(stats_path.read_text(encoding="utf-8"))

    return output_dir, stats
