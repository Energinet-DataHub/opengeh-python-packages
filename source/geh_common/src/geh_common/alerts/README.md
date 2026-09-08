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
        source_key_column="calculation_id",
        target_dataframe=published_dataframe,
        target_key_column="calculation_id",
        comparison_type=ComparisonType.MISSING_RECORDS,
        heading="Calculated records missing from published results",
        summary="calculated records are missing from published results",
    ),
    ComparisonResult(
        source_dataframe=published_dataframe,
        source_key_column="calculation_id",
        target_dataframe=calculated_dataframe,
        target_key_column="calculation_id",
        comparison_type=ComparisonType.MISSING_RECORDS,
        heading="Published records missing from calculated results",
        summary="published records are missing from calculated results",
    ),
]

compare_and_report("Wholesale comparison failed", comparisons)
```

By default, non-empty results are printed and an email summary is sent. Use
`send_email=False` for printing only or `print_results=False` for email only.
No output or email is produced when every comparison is empty.

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
        source_key_column="calculation_id",
        target_dataframe=published_dataframe,
        target_key_column="calculation_id",
        comparison_type=ComparisonType.VALUES,
        comparison_columns=(
            ("quantity", "quantity"),
            ("quality", "quality"),
        ),
        heading="Records with mismatched values",
        summary="records have mismatched values",
    ),
)
```