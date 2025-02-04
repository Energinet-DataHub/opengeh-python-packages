from pathlib import Path

from tests.constants import UNIT_TEST_DIR

MIGRATION_TEST_DIR = Path(__file__).parent
TEST_SCRIPTS_DIR = "tests." + str(MIGRATION_TEST_DIR.relative_to(UNIT_TEST_DIR.parent) / "test_scripts").replace(
    "/", "."
)
