# Covernator

## Logic

The covernator is used to track which scenarios are covered by test.
Given a folder to search for scenario tests, the covernator searches for configuration files, that define which scenarios have to be tested
(see [all_cases_test.yml](../../../source/geh_common/tests/testing/unit/covernator/test_files/coverage/all_cases_test.yml) for an example).
This file (referred to as `main-file`) has to following this naming pattern in order for it to be discovered: `coverage/all_cases*.yml`. (a yml file in a folder called coverage that starts with `all_cases`)
Each main-file defines a group of scenarios, where the group name will be the name of the parent folder in which the coverage folder is located in. (In the example of the linked file it is `test_files`)
The file can also be located in the root folder to search for scenario groups, in this case the group would be None.

In the same folder as the `coverage` folder there needs to be a folder called `scenario_tests`, which will be used to map existing tests to the ones defined in the main-file (see [coverage_mapping.yml](../../../source/geh_common/tests/testing/unit/covernator/test_files/scenario_tests/first_layer_folder1/sub_folder/coverage_mapping.yml) for an example).
The covernator will look for files called `coverage_mapping.yml` and uses the path relative to the `scenario_tests` folder as the scenario name (e.g. `first_layer_folder1/sub_folder` in the example).

## Implementation

The implementation of the logic is located in [src/geh_common/testing/covernator](../../../source/geh_common/src/geh_common/testing/covernator), and the cli tool (that can also start a local streamlit application) is located in [src/geh_common/covernator_streamlit](../../../source/geh_common/src/geh_common/covernator_streamlit)

### Logic

The entrypoint to the logic is the function `run_covernator` in [commands.py](../../../source/geh_common/src/geh_common/testing/covernator/commands.py)

#### run_covernator

- searches all main-files that defines a group of scenarios
- loops over each group of scenarios
    - find all cases of that scenario-group that are listed in the main-file
    - find all scenarios in the `scenario_tests` folder of that group and map them with the content in the main-file
- save csv files (using polars) with all cases and all scenarios, as well as a json file to have some statistics about the amount of cases, scenarios and groups

#### find_all_cases

- given a path to a main-file
- reads in the yml-file and recursivly gets all the cases present in the file, including their path, as cases case be structured into hierarchies
- parses the cases to a list of CaseRows

#### find_all_scenarios

- looks for all `coverage_mapping.yml` files in a given path (if run from the `run_covernator` this will be the `scenario_tests` folder in the group)
- parses all implemented scenarios that are mentioned in the valid files to ScenarioRows
- in order for a file to be valid it must follow the structure mentioned in [setup/folderstructure](#setup--usage)

### Cli Tool

In order to run the covernator from the cli execute either:

- the file [server.py](../../../source/geh_common/src/geh_common/covernator_streamlit/server.py)
- a python command that imports the function (this allows to run python from a different code base just by installing geh_common in the current environment)
- or add the following to the pyproject.toml in the repository to run it from the command line

```toml
[project.scripts]
covernator = "geh_common.covernator_streamlit:main"
```

Run it with:

```sh
python -c "from geh_common.covernator_streamlit import main; main()" [-o /path/to/save/files/to] [-p /path/to/look/for/scenario_tests] [-g] [-s] [-k github-output-key]
```

```sh
covernator [-o /path/to/save/files/to] [-p /path/to/look/for/scenario_tests] [-g] [-s] [-k github-output-key]
```

Optional parameters are:

- -p / --path => this changes the folder to look for files (default `./tests`)
- -o / --output-dir => set the location where the files are being created that are used to run the streamlit app (default to a temporary folder)
- -g / --generate-only => used as a boolean flag. If provided, only files are created, but no streamlit app is running (does not make sence without defining the output_dir as the data will otherwise be lost)
- -s / --serve-only => used as a boolean flag. If provided, only runs the streamlit app without generating files (does not make sence without defining the output_dir as there won't be data to read from in a new temporary folder)
- -k / --github-output-key => if set, it will write to the github output with the provided key. The output that will be written is the statistic about the current run

## Setup & Usage

Setup the folder structure like the following (scenario_group is optional, this could also be the root-testfolder);

```plaintext
├── scenario_group
    ├── coverage/
        ├── all_cases_scenario_group.yml
    └── scenario_tests/
        ├── given_something/
            └── coverage_mapping.yml
        └── given_another_thing/
            └── can_contain_multiple_layers/
                └── coverage_mapping.yml
```

run the cli-tool locally, in ci or use the according github-action in the ci like this:

```yaml
jobs:
  covernator:
    runs-on: ubuntu-latest
    steps:
      - name: Covernator
        uses: Energinet-DataHub/.github/.github/actions/python-covernator-generate-files@vFILLME
        with:
          project_name: {{ project_name }}
          project_directory: {{ project_directory }}
          geh_common_version: 5.8.11
```

The `project_name` and `project_directory` can also be done with a strategy, if the step should run for multiple projects in the same repository
The default `geh_common_version` of 5.8.11 can be overwritten (although it is not necessary). If there is an update to the covernator in the future, the version either has to be overwritten in the CI that is using the action, or the default value has to be changed in the .github repository.

## Next Steps

### CI Validation

In the main-files, cases are followed by a boolean. This boolean should be true if the scenario is excpected to be implemented, and false if it is not implemented yet.
In a future update, the CI should fail if there is not a matching implementation for a case that was set to `true` in the main-file.

### Presenting the changes

The covernator generates files that are currently saved as artifacts.
In the future these files should be saved to a defined location from where it can be read by a running application.
Currently there is a streamlit app that can do that locally, by downloading the files and linking the folder.
A next step would be to save the files on successful merge to main to e.g. a storage account.
The application (e.g. streamlit) can then read from this storage account, so there can be a live application always presenting the state of the current main branch.
