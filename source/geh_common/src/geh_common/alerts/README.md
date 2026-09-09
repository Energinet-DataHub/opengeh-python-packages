# Alerts

Describe each check with a `ComparisonResult`, then pass the list to
`compare_and_report`. The function selects the comparison algorithm, counts and
prints non-empty results, sends one email summary, and cleans up cached
DataFrames.

```python
from geh_common.alerts import (
    ComparisonResult,
    ComparisonType,
    compare_and_report,
)

comparisons = [
    ComparisonResult(
        source_dataframe=calculated_dataframe,
        source_key_columns=["calculation_id"],
        target_dataframe=published_dataframe,
        target_key_columns=["calculation_id"],
        comparison_type=ComparisonType.MISSING_RECORDS,
        heading="Calculated records missing from published results",
        summary="calculated records are missing from published results",
    ),
    ComparisonResult(
        source_dataframe=published_dataframe,
        source_key_columns=["calculation_id"],
        target_dataframe=calculated_dataframe,
        target_key_columns=["calculation_id"],
        comparison_type=ComparisonType.MISSING_RECORDS,
        heading="Published records missing from calculated results",
        summary="published records are missing from calculated results",
    ),
]

compare_and_report("Wholesale comparison failed", comparisons)
```

`source_key_columns` and `target_key_columns` must be non-empty lists of the
same length. Columns are matched by position. The example above uses a single
key column. A comparison with multiple key columns can use different source and
target names:

```python
composite_key_comparison = ComparisonResult(
    source_dataframe=calculated_dataframe,
    source_key_columns=["calculation_id", "calculation_day"],
    target_dataframe=published_dataframe,
    target_key_columns=["published_calculation_id", "published_day"],
    comparison_type=ComparisonType.MISSING_RECORDS,
    heading="Calculated records missing from published results",
    summary="calculated records are missing from published results",
)
```

By default, non-empty results are printed and an email summary is sent. Use
`send_email=False` for printing only or `print_results=False` for email only.
Console output shows at most 20 rows per comparison and truncates long values.
Set `max_displayed_rows` to change the preview size. No output or email is
produced when every comparison is empty.

Email delivery reads these environment variables:

- `SENDGRID_API_KEY`
- `ALERT_EMAIL_FROM`
- `ALERT_EMAIL_TO`

## Value comparisons

The same list can include value comparisons. These require
`comparison_columns`, containing `(source_column, target_column)` pairs:

```python
comparisons.append(
    ComparisonResult(
        source_dataframe=calculated_dataframe,
        source_key_columns=["calculation_id", "calculation_day"],
        target_dataframe=published_dataframe,
        target_key_columns=["published_calculation_id", "published_day"],
        comparison_type=ComparisonType.VALUES,
        comparison_columns=(
            ("quantity", "quantity"),
            ("quality", "quality"),
        ),
        heading="Records with mismatched values",
        summary="field values differ",
    ),
)
```

Value comparisons produce one row per differing field instead of returning the
entire source record. The compact result contains `record_key`, `source_column`,
`target_column`, `source_value`, and `target_value`. Values are represented as
strings so columns with different Spark types can be included in one result. A
single-column `record_key` is that key's string value; a multiple-column
`record_key` is a JSON object containing the source key names and values.
