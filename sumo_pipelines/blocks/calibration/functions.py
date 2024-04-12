from datetime import datetime

import polars as pl
from pytz import timezone

from sumo_pipelines.blocks.calibration.config import (
    PostProcessingGEHConfig,
    USDOTCalibrationConfig,
)

# if Type:
#     import polars as pl
# else
#     pass


def calculate_geh(config: PostProcessingGEHConfig, *args, **kwargs):
    # Load the data
    rw_count_df = pl.read_parquet(config.rw_count_file)
    sim_df = (
        pl.read_parquet(config.sim_count_file)
        .rename({"nVehEntered": "volume", "begin": "sim_time", "id": "detector"})
        .with_columns(
            pl.col(
                [
                    "sim_time",
                    "volume",
                ]
            ).cast(pl.Float64()),
        )
        .with_columns(
            (pl.col("volume") * (3600 / config.agg_interval)).alias("hourly_volume")
        )
    )

    #

    # find the interval of each of the dataframes
    sim_agg_interval = (
        sim_df.select("sim_time").unique().sort("sim_time")["sim_time"].diff()[1]
    )
    rw_agg_interval = (
        rw_count_df.select("sim_time").unique().sort("sim_time")["sim_time"].diff()[1]
    )

    if (sim_agg_interval != config.agg_interval) and (
        rw_agg_interval != config.agg_interval
    ):
        raise ValueError(
            f"Aggregation interval does not match for both dataframes. sim: {sim_agg_interval}, rw: {rw_agg_interval}"
        )

    # check if hourly volume in rw_count_df
    rw_df_columns = rw_count_df.columns
    hourly_volume = "hourly_volume" in rw_df_columns
    if not hourly_volume:
        # make the hourly volume column
        rw_count_df = rw_count_df.with_columns(
            (pl.col("volume") * (3600 / config.agg_interval)).alias("hourly_volume")
        )

    # Join on sim_time and detector name
    combined_df = rw_count_df.join(
        sim_df, on=["sim_time", "detector"], how="inner", suffix="_sim"
    )

    # filter the low and high time
    combined_df = combined_df.filter(
        pl.col("sim_time").is_between(config.time_low_filter, config.time_high_filter)
    )

    combined_df = combined_df.with_columns(
        (
            (
                (pl.col("hourly_volume_sim") - pl.col("hourly_volume")) ** 2
                / (0.5 * (pl.col("hourly_volume_sim") + pl.col("hourly_volume")))
            )
            ** (0.5)
        ).alias("geh")
    )
    # Store the total average geh as val
    config.val = combined_df["geh"].mean()

    # Store the # locations & times below cutoff
    config.geh_ok = (
        combined_df.filter(pl.col("geh") < config.geh_threshold).shape[0]
        / combined_df.shape[0]
    )

    # Save combined dataframe if save_path is declared
    if config.output_file:
        combined_df.write_parquet(config.output_file)


def _open_detector_df(file_path: str, target_col: str):
    import polars as pl

    return (
        pl.scan_parquet(file_path)
        .rename({"nVehEntered": "volume", "begin": "sim_time", "id": "detector"})
        .with_columns(
            pl.col(["sim_time", target_col]).cast(pl.Float64),
        )
        .with_columns(
            pl.col("detector").str.split("_").list.first().alias("tl"),
            pl.col("detector")
            .str.split("_")
            .list.take(1)
            .list.first()
            .cast(int)
            .alias("detector"),
        )
    )


def usdot_table_join(
    config: USDOTCalibrationConfig,
) -> pl.DataFrame:
    sim_df = _open_detector_df(config.sim_file, config.target_col)
    rw_df = pl.scan_parquet(config.calibrate_file)

    # check that the desired aggregation interval is the same as what is in the file
    agg_interval = rw_df.select(
        (pl.col("Timestamp").unique().sort().diff() / 1e6).take(2)
    ).collect()["Timestamp"][0]
    assert (agg_interval >= config.agg_interval) & (
        (agg_interval % config.agg_interval) == 0
    )

    if isinstance(config.start_time, str):
        start_time = datetime.strptime(
            config.start_time,
            "%Y-%m-%d %H:%M:%S",
        ).replace(tzinfo=timezone("US/Central"))
    else:
        start_time = config.start_time.replace(tzinfo=timezone("US/Central"))

    sim_df = sim_df.with_columns(
        (pl.lit(start_time) + pl.col("sim_time") * 1e6)
        .cast(pl.Datetime(time_unit="us", time_zone="US/Central"))
        .alias("rw_time"),
    ).collect()

    for function_obj in config.sim_file_functions:
        sim_df = function_obj.func(sim_df, function_obj.config)

    calibrate_df = (
        (
            sim_df.groupby_dynamic(
                index_column="rw_time",
                every=f"{int(agg_interval)}s",
                by=config.join_on_cols,
            )
            .agg(pl.col(f"{config.target_col}").sum().alias(f"{config.target_col}_sim"))
            .join(
                rw_df.collect().with_columns(
                    pl.col("Timestamp").cast(sim_df["rw_time"].dtype)
                ),
                left_on=[*config.join_on_cols, "rw_time"],
                right_on=[*config.join_on_cols, "Timestamp"],
                how="inner",
            )
        )
        .with_columns(((pl.col("rw_time") - start_time) / 1e6).alias("sim_time"))
        .filter(pl.col("sim_time") > config.warmup)
    )

    if config.sql_expression:
        calibrate_df = (
            pl.SQLContext(frames={"frame": calibrate_df})
            .execute(config.sql_expression)
            .collect()
        )

    return calibrate_df


def usdot_calibrate(config: USDOTCalibrationConfig, *args, **kwargs) -> None:
    calibrate_df = usdot_table_join(config)

    target_col = f"{config.target_col}_sim"

    calibrate_df = calibrate_df.groupby([*config.join_on_cols]).agg(
        # TAT2
        (
            pl.col(target_col)
            .is_between(
                pl.col("one_sigma_lower_bound"), pl.col("one_sigma_upper_bound")
            )
            .sum()
            / pl.count()
        ).alias("one_sigma_accuracy"),
        # TAT1
        (
            pl.col(target_col)
            .is_between(
                pl.col("two_sigma_lower_bound"), pl.col("two_sigma_upper_bound")
            )
            .sum()
            / pl.count()
        ).alias("two_sigma_accuracy"),
        # TAT3
        (
            ((pl.col(target_col) - pl.col(config.target_col)).abs()).sum() / pl.count()
        ).alias("BDAE"),
        # TAT4
        ((pl.col(target_col) - pl.col(config.target_col)).sum() / pl.count())
        .abs()
        .alias("BDSE"),
        pl.col("BDAE_Threshold").first(),
        (pl.col("BDAE_Threshold") / 3).first().alias("BDSE_Threshold"),
    )

    calib_pass = (
        (calibrate_df["one_sigma_accuracy"] >= (2 / 3)).all()
        & (
            (calibrate_df["two_sigma_accuracy"] >= (0.95)).all()
            | (
                (
                    (calibrate_df.shape[0] - calibrate_df["two_sigma_accuracy"].sum())
                    <= 1
                )
                & (calibrate_df.shape[0] < 20)
            )  # USDOT TATIII says that if there are less than 20, then 1 can be outside of 2 sigma
        )
        & (calibrate_df["BDAE"] < calibrate_df["BDAE_Threshold"]).all()
        & (calibrate_df["BDSE"] < calibrate_df["BDSE_Threshold"]).all()
    )

    config.calibration_passed = calib_pass

    if config.output_file:
        calibrate_df.write_parquet(config.output_file)
