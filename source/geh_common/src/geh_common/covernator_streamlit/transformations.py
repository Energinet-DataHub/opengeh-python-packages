import polars as pl


def combine_cases_with_scenarios(cases: pl.DataFrame, scenarios: pl.DataFrame) -> pl.DataFrame:
    return cases.join(scenarios, left_on="TestCase", right_on="CaseCoverage", how="left")


def get_coverage_stats(combined: pl.DataFrame) -> pl.DataFrame:
    return (
        (
            combined.group_by(["Group", "Path"])
            .agg(
                pl.count().alias("Total"),
                pl.col("Scenario").drop_nulls().alias("Scenarios"),
                pl.col("Scenario").null_count().alias("NotCovered"),
            )
            .with_columns(
                pl.col("Scenarios").list.len().alias("Covered"),
            )
        )
        .sort(["Group", "Path"])
        .select(["Group", "Path", "Total", "Covered", "NotCovered", "Scenarios"])
    )


def get_non_covered_cases(combined: pl.DataFrame) -> pl.DataFrame:
    return combined.filter(pl.col("Scenario").is_null()).select(["Group", "Path", "TestCase"])
