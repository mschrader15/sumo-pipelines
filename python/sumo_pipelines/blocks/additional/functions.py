import polars as pl


from .config import WayPointConfig

# TODO: Implement the following function


# def generate_waypoints(config, global_config, *args, **kwargs) -> None:

#     df = pl.read_parquet(config.trajectory_file)

#     xml = f"""<additional>
#         <vehicle id="follower" depart="0" departPos="">


def generate_waypointed_route(
    config: WayPointConfig,
    *args,
    **kwargs,
) -> None:
    # this function reads in the trajectory to way point and then creates a vehicle route file with waypoints
    df = pl.read_parquet(config.trajectory_file)

    path = []

    df.with_columns(pl.col("lane_position").shift(-1).alias("stopPostition")).select(
        pl.struct(
            pl.col("lane_position"),
            pl.col("time"),
            pl.col("speed"),
            pl.col("lane"),
            pl.col("stopPostition"),
        ).map_elements(
            lambda x: f'<stop lane="{x["lane"]}" startPos="{x["lane_position"]}" stopPos="{x["stopPosition"]}" arrival="{x["time"]}" speed="{x["speed"]}"/>'
        )
    )
