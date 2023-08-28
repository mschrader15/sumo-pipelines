from sumo_pipelines.blocks.calibration.config import PostProcessingGEHConfig


def calculate_geh(config: PostProcessingGEHConfig, *args, **kwargs):
    import polars as pl

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
                (
                    (pl.col("hourly_volume_sim") - pl.col("hourly_volume")) ** 2
                    / (0.5 * (pl.col("hourly_volume_sim") + pl.col("hourly_volume")))
                )
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
