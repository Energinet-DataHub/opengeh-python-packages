from pathlib import Path

from testcommon.covernator import TestCases
from tests.data.cases import Cases
from tests.constants import TEST_DATA_DIR, TEST_DIR


def test_submodule_discovery():
    assert TestCases.__subclasses__() == [Cases]
    assert TestCases.get_subclass_paths()[0].relative_to(
        TEST_DATA_DIR
    ) == Path("cases.py")


def test_find_imports_with_root_dir():
    import_paths = TestCases.find_imports(TEST_DIR / "data")
    assert import_paths.keys() == {"Cases"}
    cases_import_path = import_paths["Cases"]
    for path, cases in cases_import_path.items():
        if not path.parent == TEST_DATA_DIR:
            continue
        assert path.relative_to(TEST_DATA_DIR) == Path("import_cases.py")
        for case in cases:
            assert case.startswith("Cases.")


def test_find_imports_without_root_dir():
    import_paths = TestCases.find_imports()
    assert import_paths.keys() == {"Cases"}
    cases_import_path = import_paths["Cases"]
    for path, cases in cases_import_path.items():
        if not path.parent == TEST_DATA_DIR:
            continue
        assert path.relative_to(TEST_DATA_DIR) == Path("import_cases.py")
        for case in cases:
            assert case.startswith("Cases.")
