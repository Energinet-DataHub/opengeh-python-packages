import shutil
from pathlib import Path


class MockDBUtils:
    @property
    def fs(self):
        class MockFS:
            def ls(self, path):
                return [f for f in Path(path).iterdir()]

            def mv(self, src: str | Path, dst: str | Path):
                src = str(src)
                dst = str(dst)
                path, *rest = src.split(":", 1)
                if len(rest) > 0:
                    src = "".join(rest)
                else:
                    src = path
                path, *rest = dst.split(":", 1)
                if len(rest) > 0:
                    dst = "".join(rest)
                else:
                    dst = path
                shutil.move(Path(src), Path(dst))

            def cp(self, src, dst):
                if Path(src).is_dir():
                    shutil.copytree(src, dst)
                else:
                    shutil.copy(src, dst)

        return MockFS()
