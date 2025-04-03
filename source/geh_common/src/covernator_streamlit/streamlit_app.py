from argparse import ArgumentParser
from pathlib import Path
from tempfile import mkdtemp
from typing import Tuple

import polars as pl
import streamlit as st

from geh_common.testing.covernator import run_covernator


def _get_args() -> Tuple[Path, Path]:
    parser = ArgumentParser(description="Covernator Streamlit")
    parser.add_argument(
        "-p",
        "--path",
        default="./tests",
        help="base path to search for test scenarios. Default to './tests'",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        default=None,
        help="output directory to store the files. Default to a temporary directory",
    )
    args = parser.parse_args()
    base_path = Path(args.path)
    if args.output_dir is None:
        output_dir = Path(mkdtemp())
    else:
        output_dir = Path(args.output_dir)
    return base_path, output_dir


def my_streamlit():
    base_path, output_path = _get_args()

    st.title("Covernator Streamlit")

    st.write(f"Base Path to look for scenarios: {base_path.absolute()}")
    st.write(f"Path to store files in: {output_path.absolute()}")

    run_covernator(output_path, base_path)

    all_cases = pl.read_csv(output_path / "all_cases.csv")
    all_scenarios = pl.read_csv(output_path / "case_coverage.csv")

    st.write("### All Cases")
    st.dataframe(all_cases)

    st.write("### All Scenarios")
    st.dataframe(all_scenarios)


if __name__ == "__main__":
    my_streamlit()
