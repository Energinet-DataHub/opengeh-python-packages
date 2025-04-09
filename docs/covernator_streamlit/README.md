# Covernator Streamlit

Add the following to the pyproject.toml in the repository that uses covernator to scan QA tests and test coverage:

```toml
[project.scripts]
covernator = "geh_common.covernator_streamlit:main"
```

Run it with:

```sh
covernator [-o /path/to/save/files/to] [-p /path/to/look/for/scenario_tests] [-g] [-s]
```

Optional parameters are:

- -t / -p / --test-folder-path => this changes the folder to look for files (default `./tests`)
- -o / --output-dir => set the location where the files are being created that are used to run the streamlit app (default to a temporary folder)
- -g / --generate-only => used as a boolean flag. If provided, only files are created, but no streamlit app is running (does not make sence without defining the output_dir as the data will otherwise be lost)
- -s / --serve-only => used as a boolean flag. If provided, only runs the streamlit app without generating files (does not make sence without defining the output_dir as there won't be data to read from in a new temporary folder)
