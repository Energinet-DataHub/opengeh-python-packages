# 🧪 Covernator: Test Coverage Mapping Tool

> **CI Tool** for mapping implemented test scenarios to defined master test cases using YAML-based definitions.
> Runs in CI and outputs structured test coverage reports as CSV and JSON.

---

## 🧩 CI Automation: Coverage Markdown Generation

This process automatically runs on every **push to a pull request** within a **domain repository** (for example, _Wholesale_ or _Measurements_).
It ensures that the `docs/covernator/coverage_overview.md` file stays up to date and consistent across all branches.

---

### 🔁 Overview

| Step | Description |
|------|--------------|
| 1️⃣ | A **CI workflow** (`.github/workflows/ci-orchestrator.yml`) in the domain repo triggers when code is pushed to a pull request. |
| 2️⃣ | The workflow checks for changes via `detect-changes.yml` — if test or YAML files are modified, it sets the `covernator` flag to `true`. |
| 3️⃣ | When that flag is set, the job `covernator_commit` executes. It runs a shared GitHub Action from the central `.github` repo: `.github/actions/python-covernator-generate-files/action.yml`. |
| 4️⃣ | The action installs the correct version of the **`geh_common`** package (defined by `geh_common_version`), runs `geh_common/testing/covernator/commands.py`, and generates structured coverage results. |
| 5️⃣ | These results are fed into the **Markdown generator**, which writes `docs/covernator/coverage_overview.md`. |
| 6️⃣ | If the generated Markdown differs from what’s currently in the feature branch, it’s automatically committed back to that branch using the `stefanzweifel/git-auto-commit-action@v5` action. |

---

### ⚙️ Implementation Details

#### 🧩 CI Orchestrator (in domain repository)

Example from **`ci-orchestrator.yml`** in the _Wholesale_ domain:

```yaml
covernator_commit:
  name: Generate & Commit Covernator Results
  needs: changes
  if: ${{ needs.changes.outputs.covernator == 'true' }}
  runs-on: ubuntu-latest

  steps:
    - uses: actions/create-github-app-token@v2
      name: Generate Github token
      id: generate_token
      with:
        app-id: ${{ vars.dh3serviceaccount_appid }}
        private-key: ${{ secrets.dh3serviceaccount_privatekey }}

    - name: Run shared Covernator Commit action
      uses: Energinet-DataHub/.github/.github/actions/python-covernator-generate-files@actions/v1
      with:
        project_directory: source/geh_wholesale
        geh_common_version: 7.2.4
        github_token: ${{ steps.generate_token.outputs.token }}
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

The reusable action `.github/actions/python-covernator-generate-files/action.yml` performs the following sequence:

- Checks out the domain repository.
- Sets up a **Python 3.x** environment and installs **uv**.
- Installs the specified **geh_common** package version from GitHub.
- Runs the Covernator analysis and Markdown generation via:

```bash
geh_common/testing/covernator/entrypoints.py
```

- The process scans test definitions and generates structured coverage data.
- The Markdown generator produces or updates docs/covernator/coverage_overview.md.
- Any updated file is automatically committed using stefanzweifel/git-auto-commit-action@v5
- The commit occurs only if the generated Markdown differs from the feature branch HEAD.

## 📂 Expected Directory Layout in Domain Repo

```plaintext
<source>/
├── tests/
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