# # /// script
# # dependencies = [
# #   "streamlit",
# #   "polars",
# #   "geh_common @ git+https://git@github.com/Energinet-DataHub/opengeh-python-packages@geh_common_5.8.10#subdirectory=source/geh_common",
# # ]
# # ///
# import os
# import sys
# from pathlib import Path

# import polars as pl
# import streamlit as st

# from geh_common.covernator_streamlit.transformations import (
#     combine_cases_with_scenarios,
#     get_coverage_stats,
#     get_non_covered_cases,
# )


# def main():
#     from streamlit.cli import main

#     sys.argv = ["streamlit", "run", __file__]
#     sys.exit(main())


# # output path to save the folder in uses the environment variable OUTPUT_PATH
# # the default value will be replaced by a temporary (or specified) folder when running it locally
# OUTPUT_PATH = os.getenv("OUTPUT_PATH", "{SUBSTITUTTED_OUTPUT_PATH}")


# def run_covernator_streamlit():
#     st.set_page_config(layout="wide")
#     output_path = Path(OUTPUT_PATH)

#     st.title("Covernator Streamlit")
#     st.write(f"Path to store files in: {output_path.absolute()}")

#     assert Path(f"{output_path}/all_cases.csv").exists(), "Please run the Covernator first"
#     assert Path(f"{output_path}/case_coverage.csv").exists(), "Please run the Covernator first"

#     all_cases = pl.read_csv(output_path / "all_cases.csv")
#     all_scenarios = pl.read_csv(output_path / "case_coverage.csv")

#     combined = combine_cases_with_scenarios(all_cases, all_scenarios)

#     st.write("## Cases")

#     with st.container():
#         st.write("### Stats")
#         st.dataframe(get_coverage_stats(combined))

#     with st.container():
#         (all_cases_col, non_covered_cases_col) = st.columns([1, 1])
#         all_cases_col.write("### All Cases")
#         all_cases_col.dataframe(all_cases)

#         non_covered_cases_col.write("### Non Covered Cases")
#         non_covered_cases_col.dataframe(get_non_covered_cases(combined))

#     st.write("## Scenarios")
#     st.write("### All Scenarios")
#     st.dataframe(all_scenarios)


# if __name__ == "__main__":
#     run_covernator_streamlit()
