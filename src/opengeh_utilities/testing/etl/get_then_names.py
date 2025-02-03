import inspect
from pathlib import Path


def get_then_names(scenario_path=None) -> list[str]:
    """
    Retrieves a list of file paths for the CSV files in the `/then` folder for a scenario test.
    If 'scenario_path' is provided, it will be utilized to locate the '/then' files.
    Otherwise, the function will dynamically locate the files by inspecting the call stack for the caller.

    Args:
        scenario_path (str, optional): Path to the scenario folder. Defaults to None.

    Returns:
        list[str]: A list of file paths relative to the `then` folder, without file extensions.
    """

    if scenario_path is not None:
        output_folder_path = Path(scenario_path)
    else:
        output_folder_path = Path(inspect.stack()[1].filename).parent

    then_output_folder_path = output_folder_path / "then"

    if not output_folder_path.exists():
        raise FileNotFoundError(
            f"Could not find the 'then' folder in {output_folder_path}"
        )
    then_files = list(then_output_folder_path.rglob("*.csv"))
    if not then_files:
        raise FileNotFoundError(
            f"Could not find any CSV files in the 'then' folder in {output_folder_path}"
        )
    return [
        str(f.relative_to(then_output_folder_path).with_suffix("")) for f in then_files
    ]
