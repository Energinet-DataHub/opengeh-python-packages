import inspect
from pathlib import Path


def get_then_names(scenario_path = None) -> list[str]:
    """Get the names of the CSV files in the `then` folder in an ETL test.

    Returns:
        list[str]: A list of file paths relative to the `then` folder, without file extensions.
    """

    if scenario_path is not None:
        test_file_path = scenario_path
        output_folder_path = Path(test_file_path)
    else:
        test_file_path = inspect.stack()[1].filename
        output_folder_path = Path(test_file_path).parent

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
    return [str(f.relative_to(then_output_folder_path).with_suffix('')) for f in then_files]
