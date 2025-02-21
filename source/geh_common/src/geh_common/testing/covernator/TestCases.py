from __future__ import annotations

import importlib
from pathlib import Path


class TestCases:
    __test__ = False

    @classmethod
    def get_subclass_paths(cls) -> list[Path]:
        """Get the paths of the subclasses of the TestCases class.

        Returns:
            list[Path]: A list of the paths of the subclasses.
        """
        subclasses = cls.__subclasses__()
        paths = []
        for sub in subclasses:
            mod = importlib.import_module(sub.__module__)
            if not mod.__file__:
                raise ImportError(f"Could not find the file path of {sub.__module__}")
            paths.append(Path(mod.__file__))
        return paths

    @classmethod
    def find_imports(cls, root_dir: Path | None = None) -> dict[str, dict[Path, list[str]]]:
        """Find the imports of the subclasses of the TestCases class.

        Args:
            root_dir (Path, optional): The root directory to search for imports.
                Defaults to None.

        Returns:
            dict[str, dict[Path, list[str]]]: A dictionary of the imports of the subclasses.
        """
        if root_dir is None:
            root_dir = cls._find_git_root()
        subclasses = cls.__subclasses__()
        import_paths = {}
        for sub in subclasses:
            module_name = sub.__name__
            import_paths[module_name] = cls._find_module_imports(module_name, root_dir)
        return import_paths

    @staticmethod
    def _find_git_root() -> Path:
        current_dir = Path(__file__).parent
        depth = 0
        while current_dir != Path("/"):
            depth += 1
            if (current_dir / ".git").exists():
                return current_dir
            current_dir = current_dir.parent
            if depth > 20:
                break
        raise FileNotFoundError("Could not find the git root directory. Is this a git repository?")

    @staticmethod
    def _find_module_imports(module_name: str, root_dir: Path | str = ".") -> dict[Path, list[str]]:
        imports = {}
        for file_path in Path(root_dir).rglob("*.py"):
            with open(file_path, encoding="utf-8") as f:
                for line in f:
                    if line.startswith(module_name):
                        imports[file_path] = imports.get(file_path, [])
                        imports[file_path].append(line.strip())
        return imports
