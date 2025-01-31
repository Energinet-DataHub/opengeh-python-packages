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
We prefix scenario names with `given_...` to enhance readability. As if we ready it line-by-line it makes sense.
This is best shown with an example.

```plaintext
├──given_energy_scenario
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

#### 2) conftest.py

The `conftest.py` file is a special pytest file where `fixtures` can be defined. `Fixtures` are small function that will
run
before the actual test runs (i.e. the `test_output.py`). As such we can use it for setting up the testing environment
and
therefore make the actual tests small and concise (i.e. limiting code duplication).

Each `fixture` will have a `scope` defining how long the fixture is active and when it is set up and torn down.
There are different types:

1. Function: the default scope, the fixture is destroyed at the end of the test.
1. Class: the fixture is destroyed during teardown of the last test in the class.
1. Module: the fixture is destroyed during teardown of the last test in the module.
1. Package: the fixture is destroyed during teardown of the last test in the package.
1. Session: the fixture is destroyed at the end of the test session.

We use the `conftest.py` to make a fixtures that performs 3 steps:

1. Read `/when` file(s)
1. Perform transformation(s)
1. Construct a list `TestCases` containing `TestCase` object(s)

The `TestCase` objects consist of two things.
(a) A dataframe containing the transformed data.
(b) A path for the expect csv file (i.e. the `/then` file).

This means that the only thing we have left to do it to pass the `TestCases` object to the test files (`test_output.py`)
to compare actual & expected from the `TestCase` objects.

An example of how a `conftest.py` can look:

```python
@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest):
    # Setup
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/metering_point_periods.csv",
        metering_point_periods_schema,
    )

    # Mock the output
    migrations_wholesale_repository = Mock()
    wholesale_internal_repository = Mock()
    migrations_wholesale_repository.read_metering_point_periods.return_value = (
        metering_point_periods
    )


    # Execute the calculation logic
    calculation_output = CalculationCore().execute(
        calculation_args,
        PreparedDataReader(
            migrations_wholesale_repository,
            wholesale_internal_repository,
        ),
    )

    # Construct the TestCases object
    return TestCases(
        [
            TestCase(
                expected_csv_path=f"{scenario_path}/then/output.csv",
                actual=calculation_output.basis_data_output.grid_loss_metering_points,
            ),
        ],
    )
```

Notice, we are using `scope = 'module'`, meaning all the tests within a module will reuse the calculation defined in the `conftest`. This approach will result in a reduction in overall running time.

#### 3) test_output.py

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

We utilize the fixture `test_cases`, which is responsible for performing reads and transformations. Then we perform a
lookup `test_cases[name]` and call the `assert_dataframes_and_schemas` function to compare the actual dataframe to the
csv file.

Notice, that we are using `@pytest.mark.parametrize("name", get_then_names())` decorator which functions similar to a
foreach loop. In this case it calls the `get_then_names()` function to receive a list of all the `/then` files names for
the scenario. Afterward, it runs a test for each of the file names in the list.

## Installation

IMPORTANT: Remember to fill in the newest version in the url.

```bash
pip install git+https://git@github.com/Energinet-DataHub/opengeh-python-packages@3.1.2#subdirectory=source/testcommon
```
