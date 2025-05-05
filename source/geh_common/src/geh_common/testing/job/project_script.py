import importlib
import logging
import tomllib
from pathlib import Path


def check_project_script_exists(project_root: Path) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    with open(project_root / "pyproject.toml", "rb") as file:
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
                missing_scripts.append(f"{script_name}: Function '{function_name}' not found in module '{module_path}'")
        except ImportError:
            missing_scripts.append(f"{script_name}: Module '{module_path}' could not be imported")

    # Report results
    if missing_scripts:
        logging.warning("Missing script entry points:")
        for script in missing_scripts:
            logging.warning(f"  - {script}")
    else:
        logging.info("All script entry points exist!")

    # Fail the test if any scripts are missing
    assert not missing_scripts, "Missing script entry points detected"