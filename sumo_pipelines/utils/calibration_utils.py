from typing import List

import polars as pl
from cachetools import cached
from cachetools.keys import hashkey


def _hash_df(df: pl.DataFrame) -> int:
    return df.hash_rows().sum()


@cached(
    cache={},
    key=lambda raw_df, agg_function, *args, **kwargs: hashkey(
        _hash_df(raw_df), *args, **kwargs
    ),
)
def group_raw_df(
    raw_df, time_column, agg_interval, group_on_cols, agg_function
) -> pl.DataFrame:
    return (
        raw_df.groupby_dynamic(
            index_column=time_column,
            every=agg_interval,
            by=group_on_cols,
        )
        .agg(agg_function)
        .with_columns(
            pl.col(time_column).dt.time().alias("time"),
        )
    )

    # return _group_raw_df(_hash)


def tat_step_1(
    raw_df: pl.DataFrame,
    group_on_cols: List[str],
    agg_interval: str,
    time_column: str = "Timestamp",
    agg_function: pl.Expr = None,
    *args,
    **kwargs,
) -> pl.DataFrame:
    agg_function = agg_function if agg_function is not None else pl.col("volume").sum()

    output_name = agg_function.meta.output_name()

    # raw_df = raw_df.lazy()

    return (
        group_raw_df(
            raw_df=raw_df,
            group_on_cols=group_on_cols,
            agg_interval=agg_interval,
            time_column=time_column,
            agg_function=agg_function,
        )
        .lazy()
        .with_columns(
            # calculate the mean and standard deviation at each time across the whole input range
            pl.col(output_name)
            .mean()
            .over(["time", *group_on_cols])
            .alias(f"{output_name}_mean"),
            pl.col(output_name)
            .std()
            .over(["time", *group_on_cols])
            .alias(f"{output_name}_std"),
        )
        .with_columns(
            (
                ((pl.col(output_name) - pl.col(f"{output_name}_mean")) ** 2).sqrt()
                / pl.col(f"{output_name}_mean")
            ).alias("error")
        )
        .with_columns(
            pl.col("error")
            .sum()
            .over(pl.col(time_column).dt.date())
            .alias("daily_error")
        )
        .collect()
    )


def tat_step_2(
    step_1_df: pl.DataFrame,
    raw_df: pl.DataFrame,
    target_date: str,
    target_column: str,
    agg_interval: str,
    merge_on_cols: List[str],
    group_on_cols: List[str],
    time_column: str = "Timestamp",
    agg_function: pl.Expr = None,
) -> pl.DataFrame:
    agg_function = agg_function if agg_function is not None else pl.col("volume").sum()

    grouped_raw_df = (
        group_raw_df(
            raw_df=raw_df,
            group_on_cols=group_on_cols,
            agg_interval=agg_interval,
            time_column=time_column,
            agg_function=agg_function,
        )
        .lazy()
        .filter(pl.col(time_column).dt.date().cast(str) == target_date)
    )

    return (
        step_1_df.filter(pl.col(time_column).dt.date().cast(str) == target_date)
        .drop(target_column)
        .lazy()
        .join(
            # target_day_df.filter(pl.col(time_column).dt.date())
            grouped_raw_df.select(
                pl.col(time_column).dt.time().alias("time"),
                # time_column,
                *merge_on_cols,
                target_column,
            ),
            on=["time", *merge_on_cols],
            how="left",
        )
        .with_columns(
            pl.col(target_column).fill_null(pl.col(f"{target_column}_mean")),
            pl.col(time_column).fill_null(
                pl.col(time_column).min().dt.combine(pl.col("time"))
            ),
        )
        .with_columns(
            (pl.col(target_column) - 1.96 * pl.col(f"{target_column}_std"))
            .clip_min(0)
            .alias("two_sigma_lower_bound"),
            (pl.col(target_column) + 1.96 * pl.col(f"{target_column}_std")).alias(
                "two_sigma_upper_bound"
            ),
            (pl.col(target_column) - pl.col(f"{target_column}_std"))
            .clip_min(0)
            .alias("one_sigma_lower_bound"),
            (pl.col(target_column) + pl.col(f"{target_column}_std")).alias(
                "one_sigma_upper_bound"
            ),
        )
        .collect()
    )


def tat_step_3(
    raw_df: pl.DataFrame,
    target_column: str,
    target_date: str,
    agg_interval: str,
    merge_on_cols: List[str],
    group_on_cols: List[str],
    time_column: str = "Timestamp",
    agg_function: pl.Expr = None,
    *args,
    **kwargs,
) -> pl.DataFrame:
    agg_function = agg_function if agg_function is not None else pl.col("volume").sum()
    grouped_raw_df = group_raw_df(
        raw_df=raw_df,
        group_on_cols=group_on_cols,
        agg_interval=agg_interval,
        time_column=time_column,
        agg_function=agg_function,
    ).lazy()

    return (
        grouped_raw_df
        # step_1_df.lazy()
        .join(
            grouped_raw_df.filter(
                pl.col(time_column).dt.date().cast(str) == target_date
            ).select(
                pl.col(time_column).dt.time().alias("time"),
                time_column,
                *merge_on_cols,
                pl.col(target_column).alias(f"{target_column}_target"),
            ),
            on=["time", *merge_on_cols],
            how="left",
        )
        .filter(pl.col(time_column) != pl.col(f"{time_column}_right"))
        .drop("Timestamp_right")
        .with_columns(
            pl.col(time_column)
            .dt.date()
            .n_unique()
            .over(group_on_cols)
            .alias("n_days"),
            pl.col(time_column)
            .dt.time()
            .n_unique()
            .over(group_on_cols)
            .alias("n_intervals"),
        )
        .with_columns(
            (
                (pl.col(f"{target_column}_target") - pl.col(target_column)).abs()
                / pl.col("n_intervals")
            ).alias("BDAE"),
        )
        .with_columns(
            (pl.col("BDAE").sum().over(group_on_cols) / (pl.col("n_days") - 1)).alias(
                "BDAE_Threshold"
            ),
        )
        .select([*group_on_cols, "time", "BDAE_Threshold"])
        .unique()
        .collect()
    )
