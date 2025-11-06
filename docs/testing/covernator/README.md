# 🧪 Covernator: Test Coverage Mapping Tool

> **CI Tool** for mapping implemented test scenarios to defined master test cases using YAML-based definitions.
> Runs in CI and outputs structured test coverage reports as CSV and JSON.

---

## 🧩 CI Automation: Coverage Markdown Generation

This process automatically runs on every **push to a pull request** within a **domain repository** (for example, _Measurements_ or _Wholesale_).
It ensures that the `coverage_overview.md` file is always up to date.

#### 🔁 Overview

| Step | Description |
|------|--------------|
| 1️⃣ | The **CI workflow** in the domain repo (defined in `.github/workflows/ci-orchestrator.yml`) triggers when code is pushed to a pull request. |
| 2️⃣ | The workflow invokes the composite GitHub Action located at `.github/actions/python-covernator-generate-files/action.yml`. |
| 3️⃣ | That action installs and imports the **`geh_common`** package. It then runs the script `geh_common/testing/covernator/commands.py`. |
| 4️⃣ | `commands.py` scans all test definitions (`all_cases.yml`, `coverage_mapping.yml`, etc.), performs validation, and generates structured results. These results are then passed into the **Markdown generator** (`markdown_generator.py`). |
| 5️⃣ | The Markdown generator creates the file `docs/covernator/coverage_overview.md`, summarizing test coverage, scenarios, and errors. |
| 6️⃣ | The CI compares this generated Markdown against the latest version in the feature branch. If differences are found, it automatically commits and pushes the updated file to the same branch. |

---

### ⚙️ Enabling Covernator on a Feature Branch (e.g. "Wholesale" or "Measurements")

Covernator can be enabled on any feature branch to automatically generate and commit an updated test coverage summary.
This process integrates with the CI orchestrator and detects relevant changes to test or YAML definition files.

---

#### 🧩 1. CI Orchestration

The workflow file `.github/workflows/ci-orchestrator.yml` defines the job `covernator_commit`.
This job runs **only when the change-detection step sets the `covernator` flag to true**:

```yaml
covernator_commit:
  name: Generate & Commit Covernator Results (Test)
  needs: changes
  if: ${{ needs.changes.outputs.covernator == 'true' }}
  runs-on: ubuntu-latest

  steps:
    - name: Run shared Covernator Commit action
      uses: Energinet-DataHub/.github/.github/actions/python-covernator-generate-files@claus/covernator_action
      with:
        project_directory: source/geh_wholesale
        geh_common_version: claus/covernator_final
```

This step installs dependencies, runs the Covernator engine, generates the coverage markdown,
and commits the update if the content differs from the previous version on the branch.

#### 🧩 2. Change Detection Logic

The .github/workflows/detect-changes.yml file determines when to trigger Covernator.
It outputs a covernator flag when files matching these patterns are modified:

```yaml
covernator:
  - 'source/geh_wholesale/tests/**/*.yml'
  - 'source/geh_wholesale/tests/**/test_*.py'
```

#### 🧩 3. Shared GitHub Action

The reusable action .github/actions/python-covernator-generate-files/action.yml performs the following sequence:

1. Checks out the repository.
2. Sets up a Python 3.11 environment using uv.
3. Installs the appropriate geh_common package version.
4. Executes:

```bash
geh_common/testing/covernator/commands.py
```

— which scans test definitions and generates structured coverage data.
5. Calls the Markdown generator to produce or update docs/covernator/coverage_overview.md
6. If the file content differs from the current HEAD of the feature branch, the workflow commits and pushes the update automatically.

## 📂 Expected Directory Layout in Domain Repo

```plaintext
<repo_root>/
├── tests/
│   ├── coverage/                         # Optional: single-group mode
│   │   └── all_cases.yml
│   ├── group_x/
│   │   ├── coverage/
│   │   │   └── all_cases_group_x.yml
│   │   └── scenario_tests/
│   │       └── given_group_x_scenario1/
│   │           └── coverage_mapping.yml
│   ├── group_y/
│   └── ...
```

---

## 🔍 Definitions

| Term          | Description |
|---------------|-------------|
| **Group**     | A logical test subsystem, represented by a subfolder under `tests/`. Can be nested (`group_x/subgroup_z`). |
| **Case**      | An entry in `all_cases.yml` (or `all_cases_*.yml`) representing a business-relevant test case. |
| **Scenario**  | A test implementation defined by a folder containing `coverage_mapping.yml`, inside `scenario_test(s)/`. |
| **Covered Case** | A case explicitly listed with `true` in a scenario’s `coverage_mapping.yml` |

---

## 📜 Business Rules

### ✅ Group Identification

- Any subfolder inside `tests/` with a `coverage/` folder containing `all_cases*.yml` files is treated as a **group**.
- If `tests/coverage/` exists directly, it is treated as a **singleton group**.
- Group name is recorded as relative to `tests/`, e.g., `geh_repo3/group_x`.

---

### ✅ Case Collection

- All YAML files matching `all_cases*.yml` are loaded recursively within a group.
- YAML must use the format:

```yaml
"Main Heading":
  "Sub Heading":
    "Case ID": true
```

- **Flattened path** becomes the case's hierarchical name:
  e.g., `"Main Heading / Sub Heading"`.

---

### ✅ Scenario Discovery

- Scenario folders must be located under either `scenario_test/` or `scenario_tests/`
- All nested folders containing a `coverage_mapping.yml` are considered scenarios.

---

### ✅ Coverage Mapping

- Inside each scenario folder, `coverage_mapping.yml` must contain:

```yaml
"Case 1": true
"Case 2": false  # allowed, but not counted
```

- Only `true` entries are considered implemented coverage.
- Case names are normalized (whitespace stripped).

---

### ✅ Output Files

#### 📄 `all_cases.csv`

| Column        | Description |
|---------------|-------------|
| Group         | Group name (e.g., `geh_repo1`) |
| TestCase      | Case identifier |
| Path          | Heading hierarchy |
| Implemented   | `true` if covered in any scenario |

---

#### 📄 `case_coverage.csv`

| Column        | Description |
|---------------|-------------|
| Group         | Group name |
| Scenario      | Scenario folder name |
| CaseCoverage  | Covered case name |

---

#### 📄 `stats.json`

```json
{
  "total_cases": 18,
  "total_scenarios": 5,
  "total_groups": 3,
  "logs": {
    "error": [...],
    "info": [...]
  }
}
```

---

## ❌ Error & Validation Rules

All errors are logged in `stats["logs"]["error"]` but **do not halt execution**.

| Rule | Description |
|------|-------------|
| ❗ `Duplicate items in all cases` | Duplicate case keys in `all_cases.yml` |
| ❗ `Duplicate items in scenario` | Duplicate keys in a scenario’s `coverage_mapping.yml` |
| ❗ `Missing all_cases YAML` | Scenario folder exists, but no `all_cases*.yml` is present |
| ❗ `Missing scenario_test(s)` | Group has no scenario folder |
| ❗ `Missing coverage_mapping.yml` | Scenario folder lacks mapping file |
| ❗ `Case not covered in any scenario` | Master case never marked `true` in any scenario |
| ❗ `Case marked as false in master list` | Scenario sets a case to `false` that exists in master |
| ❗ `No scenarios found` | No valid `coverage_mapping.yml` found anywhere under group |

---

## 💬 Informational Logs

Logged under `stats["logs"]["info"]`.

| Message | Trigger |
|---------|---------|
| `Case is marked as false in master list` | Indicates test skipped intentionally |
| `Processing group: <name>` | Marks which group is currently being processed |
| `run_covernator() started!` | Logged at start |

---

## 🧪 Test Suite Coverage

| Test File | Description |
|-----------|-------------|
| `test_covernator_repo1_happy_path.py` | Single group, flat layout |
| `test_covernator_repo2_happy_path.py` | Multiple groups, separate coverage trees |
| `test_covernator_repo3_error_handling.py` | Exhaustive failure mode simulation (all errors triggered) |

---

## 🧠 Design Decisions

| Design Choice | Reason |
|---------------|--------|
| Use of Polars | Performance, consistent schema |
| Log-first error reporting | CI pipelines prefer structured logs over exceptions |
| Flexible folder depth | Accommodates nested and monorepo-style test layouts |
| `stats.json` output | Enables coverage dashboards and fail-safe assertions |

---

## 🧭 run_covernator Logic (Expanded)

The entrypoint to the tool is the function `run_covernator()`:

1. Recursively searches for all `coverage/all_cases*.yml` files.
2. Treats each of these as a test **group**.
3. For each group:
    - Loads all master cases from the YAML files.
    - Searches under `scenario_tests/` or `scenario_test/` for scenarios.
    - Maps coverage for each case found.
4. Generates:
    - `all_cases.csv`
    - `case_coverage.csv`
    - `stats.json`

---

### 🧪 find_all_cases()

- Parses a `all_cases*.yml` file.
- Recursively traverses nested sections to flatten all `case: true/false` entries.
- Captures path hierarchy (e.g. `Heading / Subheading`) along with group name.
- Returns a list of `CaseRow` objects (used in Polars CSV export).

---

### 🔍 find_all_scenarios()

- Recursively scans for `coverage_mapping.yml` files under `scenario_tests/` or `scenario_test/`.
- Uses the path **relative to** `scenario_tests/` as the scenario identifier.
- Skips files not matching YAML structure (i.e. dictionary of case: bool).

---

## 🚀 CLI Usage (Optional)

Run via Python directly:

```bash
python -c "from geh_common.covernator_streamlit import main; main()" --output-dir ./out --path ./tests
```

Or define in `pyproject.toml`:

```toml
[project.scripts]
covernator = "geh_common.covernator_streamlit:main"
```

CLI flags:

- `--path` (default: `./tests`)
- `--output-dir` (default: temp dir)
- `--generate-only`: skip UI
- `--serve-only`: skip generation
- `--github-output-key`: inject stats to GitHub Actions output

---

## 🧪 GitHub Actions Integration

```yaml
jobs:
  covernator:
    runs-on: ubuntu-latest
    steps:
      - name: Covernator
        uses: Energinet-DataHub/.github/.github/actions/python-covernator-generate-files@v5.8.11
        with:
          project_name: your-project
          project_directory: path/to/project
```

- Replace `project_name` and `project_directory` as needed.
- You can override the default `geh_common_version` (default = `5.8.11`).
