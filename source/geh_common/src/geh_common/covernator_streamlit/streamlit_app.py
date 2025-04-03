import os
from pathlib import Path

import polars as pl
import streamlit as st

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "{SUBSTITUTTED_OUTPUT_PATH}")


def my_streamlit():
    output_path = Path(OUTPUT_PATH)

    st.title("Covernator Streamlit")
    st.write(f"Path to store files in: {output_path.absolute()}")

    assert Path(f"{output_path}/all_cases.csv").exists(), "Please run the Covernator first"
    assert Path(f"{output_path}/case_coverage.csv").exists(), "Please run the Covernator first"

    all_cases = pl.read_csv(output_path / "all_cases.csv")
    all_scenarios = pl.read_csv(output_path / "case_coverage.csv")

    st.write("### All Cases")
    st.dataframe(all_cases)

    st.write("### All Scenarios")
    st.dataframe(all_scenarios)


if __name__ == "__main__":
    my_streamlit()
