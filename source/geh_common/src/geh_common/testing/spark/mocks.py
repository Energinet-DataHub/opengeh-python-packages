from __future__ import annotations

import os
import shutil
from pathlib import Path


def _sanitize_path(path: str | Path) -> Path:
    """Remove the prefix from the path if it exists."""
    path, *rest = str(path).split(":", 1)
    if len(rest) > 0:
        return Path("".join(rest))
    else:
        return Path(path)


class MockDBUtils:
    def __init__(self) -> None:
        self.fs = _MockFS()
        self.secrets = _MockSecrets()


class _MockFileInfo:
    def __init__(self, p: Path) -> None:
        path = _sanitize_path(p)
        self.path = str(path)
        self.name = path.name
        self.size = path.stat().st_size if path.exists() else 0
        self.modificationTime = int(path.stat().st_mtime * 1000) if path.exists() else 0


class _MockSecrets:
    def __init__(self):
        pass

    def get(self, scope, name):
        return os.environ.get(name)


class _MockFS:
    """Mock implementation of DBUtils FileSystem API.

    See more at https://docs.databricks.com/aws/en/dev-tools/databricks-utils#file-system-utility-dbutilsfs
    """

    def __init__(self):
        pass

    def put(self, path: str, content: str, overwrite: bool = False):
        _path = _sanitize_path(path)
        if not overwrite and _path.exists():
            raise FileExistsError(f"File {_path} already exists.")
        _path.write_text(content, encoding="utf-8")

    def mkdirs(self, path: str):
        _path = _sanitize_path(path)
        Path(_path).mkdir(parents=True, exist_ok=True)

    def ls(self, path: str):
        _path = _sanitize_path(path)
        return [_MockFileInfo(f) for f in Path(_path).iterdir()]

    def mv(self, src: str, dst: str, recurse: bool = False):
        _src = _sanitize_path(src)
        _dst = _sanitize_path(dst)
        if not Path(_src).exists():
            raise FileNotFoundError(f"Source path {src} does not exist.")
        self.cp(str(_src), str(_dst), recurse)
        self.rm(str(_src), recurse)

    def cp(self, src: str, dst: str, recurse: bool = False):
        _src = _sanitize_path(src)
        _dst = _sanitize_path(dst)
        copy_func = shutil.copytree if recurse else shutil.copy
        copy_func(_src, _dst)

    def rm(self, path: str, recurse: bool = False):
        _path = _sanitize_path(path)
        deletion_func = shutil.rmtree if recurse else os.remove
        deletion_func(_path)
