# Testcommon utility tool

This python packages includes tools and utility function for making testing easier.

## Usage

### Scenario testing

To use testcommon for the scenario testing we need to set up three things.

#### 1) Use the standard folder structure setup

```plaintext
├── scenario_name/
    ├── when/
        ├── input1.csv
        └── ...
    └── then/
        ├── output1.csv
        └── ...
    └── test_output.py
└── conftest.py
```

Input files are stored in the `/when` subfolder, while the expected results are stored in the `/then` subfolder.
Scenario names prefixed with `given_...` to enhance readability. This structure allows us to read it line-by-line,
forming a complete sentence that makes logical sense.

```plaintext
├── given_energy_scenario
    ├── when/
        ├── energy.csv
        ├── energy_per_brp.csv
        └── energy_per_es.csv
    └── then/
        └── energy_v1.csv
```

Notice, that there is not a one-to-one relation between the `/then` and `/when` files.
This is because we may have to perform various transformations, such as aggregating or joining tables, to achieve the
desired
output. (i.e., `/then` file).

#### 2) conftest

The test setup might seem unusual at first glance, as the individual test files do not perform any transformations
but solely compare actual results to expected results. The idea behind using `fixtures` is that many of the scenario
tests requires an identical setup (i.e. catalogs, schemas, and tables). Building and tearing down these setups for every
test would waste a lot of time and resources. Additionally, specifying everything within each test would lead to code
duplication and make the tests hard to read and understand. By substituting the logic and transformations into
the `conftest.py` (a special pytest file where `fixtures` can be defined), we can achieve code simplicity, code
reusability, and reduced test execution time, especially for long-running transformations. `Fixtures` are
functions that run before the actual tests, allowing us to set up the environment, perform calculations, and execute
logic and transformations before any tests are run. We can control when the `fixtures` are set up
and torn down by setting the `scope` parameter.

These are different `scope` types:

1. Function: the default scope, the fixture is destroyed at the end of the test.
1. Class: the fixture is destroyed during teardown of the last test in the class.
1. Module: the fixture is destroyed during teardown of the last test in the module.
1. Package: the fixture is destroyed during teardown of the last test in the package.
1. Session: the fixture is destroyed at the end of the test session.

Typically, the `module` is the preffered scope choice for scenario testing (we will also use the `module` scope in the
example below), as all the then-files in a scenario test rely on the exact same transformation. Additionally, tests
should always remain independent of each other, making the `module` scope is the only appropriate choice.
By using the `module` scope it means that all the tests within a module will reuse the same
instance of the `fixture`. Consequently, we perform data loading, calculations, and transformations to prepare
actual dataframes for all the tests beforehand. Subsequently, each test file (`test_ouput.py`) utilizes this
preprocessed work to simply compare the actual to an expected dataframe.

More specifically, the `conftest.py`'s `fixture` performs these 3 steps:

1. Read `/when` file(s)
1. Perform transformation(s)
1. Construct a list `TestCases` containing `TestCase` object(s)

The `TestCase` objects consist of two things.
(a) A dataframe containing the transformed data.
(b) A path for the expect csv file (i.e. the `/then` file).

An example of how a `conftest.py` can look:

```python
@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest):
    # Setup
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    input1_df = read_csv(
        spark,
        f"{scenario_path}/when/input.csv",
        schema,
    )

    # Invoke the sut (subject under test)
    actual = transformation(input1_df, ...)

    # Construct the TestCases object
    return TestCases(
        [
            TestCase(
                expected_csv_path=f"{scenario_path}/then/output.csv",
                actual=actual,
            ),
        ],
    )
```

#### 3) test_output

For almost all scenarios the `test_output.py` file will look as follows:

```python
@pytest.mark.parametrize("name", get_then_names())
def test__equals_expected(
        test_cases: TestCases,
        name: str,
        assert_dataframes_configuration: AssertDataframesConfiguration,
) -> None:
    test_case = test_cases[name]

    assert_dataframes_and_schemas(
        actual=test_case.actual,
        expected=test_case.expected,
        configuration=assert_dataframes_configuration,
    )
```

We use the `@pytest.mark.parametrize("name", get_then_names())` decorator which functions similar to a
foreach loop. In this case, it calls the `get_then_names()` function to retrieve a list of all the `/then` files names
for the given scenario. Afterward, it will perform a test for each of the file names in the list.

To retrieve the preprocessing from the `test_cases` `fixture` (the one described above) we simply specify it as a
parameter in the function header. This will provide us a `TestCases` object, from which we can perform a lookup
`test_cases[name]` and call the `assert_dataframes_and_schemas` function to compare the actual dataframe to the csv
file.

## Installation

IMPORTANT: Remember to fill in the newest version in the url.

```bash
pip install git+https://git@github.com/Energinet-DataHub/geh-python-packages@3.1.2#subdirectory=source/testcommon
```
