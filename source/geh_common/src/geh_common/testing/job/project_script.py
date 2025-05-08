import importlib
import logging
import tomllib
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)


def assert_pyproject_toml_project_script_exists(pyproject_toml_path: Path) -> None:
    """Check if all script entry points in pyproject.toml exist in project."""
    missing_scripts = find_missing_scripts(pyproject_toml_path)

    # Log results
    if not missing_scripts:
        logger.info("All script entry points exist!")
    else:
        log_message = "Missing script entry points:"
        for script_name, error_msg in missing_scripts:
            log_message += f"\n - {script_name}: {error_msg}"
        logger.error(log_message)

    # Assert based on collection rather than string parsing
    assert not missing_scripts, "Missing script entry points detected"


def find_missing_scripts(pyproject_toml_path: Path) -> List[Tuple[str, str]]:
    """Find scripts defined in pyproject.toml that do not exist in the project.

    Returns:
        List of (script_name, error_message) tuples for missing scripts
    """
    with open(pyproject_toml_path, "rb") as file:
        pyproject = tomllib.load(file)
        project = pyproject.get("project", {})

    scripts = project.get("scripts", {})
    missing_scripts = []

    for script_name, entry_point in scripts.items():
        module_path, function_name = entry_point.split(":")
        try:
            # Attempt to import the module
            module = importlib.import_module(module_path)
            # Check if the function exists
            if not hasattr(module, function_name):
                missing_scripts.append((script_name, f"Function '{function_name}' not found in module '{module_path}'"))
        except ImportError:
            missing_scripts.append((script_name, f"Module '{module_path}' could not be imported"))

    return missing_scripts
