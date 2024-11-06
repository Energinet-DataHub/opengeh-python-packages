import os, re
from unittest.mock import Mock
import telemetry_logging.logging_configuration as config
from telemetry_logging.span_recording import span_record_exception


def do_nothing(*args, **kwargs) -> None:
    pass


def _get_code_blocks_from_readme(readme_path: str) -> list[str]:
    with open(readme_path, "r") as file:
        lines = file.readlines()
        code_blocks = []
        current_block = ""
        in_code_block = False

        for line in lines:
            if "```python" in line:  # Start of the code block found
                in_code_block = True
                continue
            if "```" in line and in_code_block:  # End of current code block found
                in_code_block = False
                code_blocks.append(current_block)
                current_block = ""
                continue
            if in_code_block:  # Currently inside a code block
                current_block += line

    return code_blocks


def _get_source_path() -> str:
    working_directory = os.getcwd()
    source_path = re.match(r"(.+?source)", working_directory).group(1)
    return source_path


def _check_code_syntax(code: str):
    try:
        byte_code = compile(code, "<string/>", "exec")
        exec(byte_code)
    except Exception as e:
        raise e
    return "Syntax is correct."


def test_read_me_python_code_example():
    # Arrange
    source_path = _get_source_path()
    readme_path = f"{source_path}/telemetry/README.md"

    # Act
    code_blocks = _get_code_blocks_from_readme(readme_path)

    # Assert
    for code_block in code_blocks:
        assert _check_code_syntax(code_block)
