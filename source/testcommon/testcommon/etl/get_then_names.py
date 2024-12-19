import inspect
from pathlib import Path


def get_then_names() -> list[str]:
    """Get the names files in the `then` folder in a feature test.

    Returns:
        list[str]: A list of file names without extensions.
    """
    test_file_path = inspect.stack()[1].filename
    output_folder_path = Path(test_file_path).parent / "then"
    return [Path(file).stem for file in output_folder_path.rglob("*.csv")]
