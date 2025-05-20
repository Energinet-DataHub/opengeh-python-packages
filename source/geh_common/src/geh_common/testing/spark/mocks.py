import shutil
from pathlib import Path


def _sanitize_path(path: str) -> str:
    """Remove the prefix from the path if it exists."""
    path, *rest = str(path).split(":", 1)
    if len(rest) > 0:
        return Path("".join(rest))
    else:
        return Path(path)


class MockDBUtils:
    @property
    def fs(self):
        class MockFS:
            def ls(self, path):
                return [f for f in Path(path).iterdir()]

            def mv(self, src: str | Path, dst: str | Path):
                shutil.move(_sanitize_path(src), _sanitize_path(dst))

            def cp(self, src, dst):
                if Path(src).is_dir():
                    shutil.copytree(_sanitize_path(src), _sanitize_path(dst))
                else:
                    shutil.copy(_sanitize_path(src), _sanitize_path(dst))

        return MockFS()
