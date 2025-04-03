import pandas as pd
import streamlit as st


# Example data
def get_scenarios():
    return [
        {"group": "A", "path": "/path1", "cases": ["case1", "case2"]},
        {"group": "B", "path": "/path2", "cases": ["case3"]},
        {"group": "A", "path": "/path3", "cases": ["case2", "case3"]},
    ]


def get_cases():
    return [
        {"group": "A", "path": "/path1", "case": "case1", "implemented": True},
        {"group": "A", "path": "/path1", "case": "case2", "implemented": False},
        {"group": "B", "path": "/path2", "case": "case3", "implemented": True},
        {"group": "A", "path": "/path3", "case": "case2", "implemented": False},
        {"group": "A", "path": "/path3", "case": "case3", "implemented": True},
    ]


# Convert scenarios to a DataFrame
scenarios = get_scenarios()
scenarios_df = pd.DataFrame(scenarios)
scenarios_df["cases"] = scenarios_df["cases"].apply(lambda x: ", ".join(x))

# Convert cases to a DataFrame
cases = get_cases()
cases_df = pd.DataFrame(cases)

# Map cases to scenarios
case_to_scenarios = {}
for scenario in scenarios:
    for case in scenario["cases"]:
        if case not in case_to_scenarios:
            case_to_scenarios[case] = []
        case_to_scenarios[case].append(scenario["path"])

cases_df["scenarios"] = cases_df["case"].map(lambda x: ", ".join(case_to_scenarios.get(x, [])))

st.title("Scenarios and Cases Viewer")

# Filters for scenarios
group_filter = st.sidebar.multiselect("Filter by Group", scenarios_df["group"].unique())
path_filter = st.sidebar.text_input("Filter by Path")

filtered_scenarios = scenarios_df.copy()
if group_filter:
    filtered_scenarios = filtered_scenarios[filtered_scenarios["group"].isin(group_filter)]
if path_filter:
    filtered_scenarios = filtered_scenarios[filtered_scenarios["path"].str.contains(path_filter, case=False, na=False)]

st.subheader("Scenarios")
st.dataframe(filtered_scenarios)

# Filters for cases
case_group_filter = st.sidebar.multiselect("Filter Cases by Group", cases_df["group"].unique())
implemented_filter = st.sidebar.selectbox("Implemented?", ["All", True, False])

filtered_cases = cases_df.copy()
if case_group_filter:
    filtered_cases = filtered_cases[filtered_cases["group"].isin(case_group_filter)]
if implemented_filter != "All":
    filtered_cases = filtered_cases[filtered_cases["implemented"] == implemented_filter]

st.subheader("Cases")
st.dataframe(filtered_cases)
