from tests.constants import TESTS_ROOT_DIR
from tests.migrations import MIGRATIONS_UNIT_TEST_DIR

RELATIVE_PATH = MIGRATIONS_UNIT_TEST_DIR.relative_to(TESTS_ROOT_DIR)
MIGRATION_TEST_SCRIPTS_DIR = "tests." + str(RELATIVE_PATH / "test_scripts").replace("/", ".")
