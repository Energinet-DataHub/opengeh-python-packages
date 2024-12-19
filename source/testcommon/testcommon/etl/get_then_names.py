import inspect
from pathlib import Path


def get_then_names() -> list[str]:
    test_file_path = inspect.stack()[1].filename
    output_folder_path = Path(test_file_path).parent / "then"
    return [Path(file).stem for file in output_folder_path.rglob("*.csv")]
