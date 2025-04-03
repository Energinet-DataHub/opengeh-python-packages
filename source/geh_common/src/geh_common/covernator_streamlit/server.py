import logging
import os
import subprocess
import tempfile
from argparse import ArgumentParser
from pathlib import Path

from geh_common.testing.covernator import run_covernator


def parse_args():
    p = ArgumentParser(description="Run the Streamlit app.")
    p.add_argument(
        "-p",
        "--path",
        default="./tests",
        help="base path to search for test scenarios. Default to './tests'",
    )
    p.add_argument(
        "-o",
        "--output_dir",
        default=tempfile.mkdtemp(),
        help="output directory to store the files. Default to a temporary directory",
    )
    p.add_argument(
        "-g",
        "--only-generate",
        default=None,
        help="Do not run the streamlit app, only generate the files. Requires --output_dir",
        action="store_true",
    )
    p.add_argument(
        "-s",
        "--only-serve",
        default=None,
        help="Do not generate the files, only run the streamlit app",
        action="store_true",
    )
    return p.parse_args()


def main():
    args = parse_args()
    base_path = Path(args.path)
    output_dir = Path(args.output_dir)
    if not args.only_serve:
        run_covernator(output_dir, base_path)
    if args.only_generate:
        return
    streamlit_script = os.path.join(os.path.dirname(__file__), "streamlit_app.py")
    with open(streamlit_script) as f:
        content = f.read()
        content = content.replace("{SUBSTITUTTED_OUTPUT_PATH}", output_dir.as_posix())
    with open(f"{output_dir}/script.py", "w") as f:
        f.write(content)
    command = ["streamlit", "run", f"{output_dir}/script.py"]
    logging.info(command)
    res = subprocess.run(command, check=True)
    if res.returncode != 0:
        logging.error("Error running streamlit app")
        raise RuntimeError("Error running streamlit app")


if __name__ == "__main__":
    main()
