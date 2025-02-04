from pathlib import Path

from geh_common.testing.covernator.TestCases import TestCases
from tests.unit.testing.covernator.constants import COVERNATOR_TEST_DATA
from tests.unit.testing.covernator.data.cases import Cases


def test_submodule_discovery():
    assert TestCases.__subclasses__() == [Cases]
    assert TestCases.get_subclass_paths()[0].relative_to(COVERNATOR_TEST_DATA) == Path(
        "cases.py"
    )


def test_find_imports_with_root_dir():
    import_paths = TestCases.find_imports(COVERNATOR_TEST_DATA)
    assert import_paths.keys() == {"Cases"}
    cases_import_path = import_paths["Cases"]
    for path, cases in cases_import_path.items():
        if not path.parent == COVERNATOR_TEST_DATA:
            raise AssertionError(f"{path} is not in {COVERNATOR_TEST_DATA}")
        assert path.relative_to(COVERNATOR_TEST_DATA) == Path("import_cases.py")
        for case in cases:
            assert case.startswith("Cases.")


def test_find_imports_without_root_dir():
    import_paths = TestCases.find_imports()
    assert import_paths.keys() == {"Cases"}
    cases_import_path = import_paths["Cases"]
    for path, cases in cases_import_path.items():
        if not path.parent == COVERNATOR_TEST_DATA:
            raise AssertionError(f"{path} is not in {COVERNATOR_TEST_DATA}")
        assert path.relative_to(COVERNATOR_TEST_DATA) == Path("import_cases.py")
        for case in cases:
            assert case.startswith("Cases.")
