# 🧪 Covernator: Test Coverage Mapping Tool

> **CI Tool** for mapping implemented test scenarios to defined master test cases using YAML-based definitions.
> Runs in CI and outputs structured test coverage reports as CSV and JSON.

---

## 📂 Expected Directory Layout

```
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
