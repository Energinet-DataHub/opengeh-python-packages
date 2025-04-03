from argparse import ArgumentParser

import streamlit as st

arg_parser = ArgumentParser()
arg_parser.add_argument(
    "--output_dir",
    type=str,
)
arg_parser.add_argument(
    "--feature_folder",
    type=str,
)
arguments = arg_parser.parse_args()

# args_to_show = []
# for arg in sys.argv:
#     args_to_show.append(arg)

st.title("The Covernator")
for arg in arguments.items():
    st.subheader(arg)
