import inspect
from pathlib import Path


def get_then_names() -> list[str]:
    """Get the names of the CSV files in the `then` folder in an ETL test.

    Returns:
        list[str]: A list of file paths relative to the `then` folder.
    """
    test_file_path = inspect.stack()[1].filename
    output_folder_path = Path(test_file_path).parent / "then"
    if not output_folder_path.exists():
        raise FileNotFoundError(
            f"Could not find the 'then' folder in {Path(test_file_path).parent}"
        )
    then_files = list(output_folder_path.rglob("*.csv"))
    if not then_files:
        raise FileNotFoundError(
            f"Could not find any CSV files in the 'then' folder in {Path(test_file_path).parent}"
        )
    return [str(f.relative_to(output_folder_path)) for f in then_files]
