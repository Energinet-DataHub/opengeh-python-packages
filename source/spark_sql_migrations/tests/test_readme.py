import re
import os
import tests.helpers.mock_helper as mock_helper
from unittest.mock import Mock
import spark_sql_migrations.container as container
import spark_sql_migrations.schema_migration_pipeline as schema_migration_pipeline


def test_read_me_python_code_example(mocker: Mock) -> None:
    # Arrange
    mocker.patch.object(
        container,
        container.create_and_configure_container.__name__,
        side_effect=mock_helper.do_nothing,
    )

    mocker.patch.object(
        schema_migration_pipeline,
        schema_migration_pipeline.migrate.__name__,
        side_effect=mock_helper.do_nothing,
    )

    source_path = _get_source_path()
    readme_path = f"{source_path}/spark_sql_migrations/README.md"

    # Act
    code_blocks = _get_code_blocks_from_readme(readme_path)

    # Assert
    for code_block in code_blocks:
        assert _check_code_syntax(code_block)


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
