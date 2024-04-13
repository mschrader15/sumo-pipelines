from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl
from omegaconf import DictConfig

from sumo_pipelines.utils.file_helpers import import_sumo_tools_module

from .config import RandomTripsConfig, RouteSamplerConfig, TurnFileConfig


def write_turn_file_input(
    turn_file_config: TurnFileConfig,
    *args,
    **kwargs,
) -> None:
    df = pl.read_parquet(turn_file_config.to_from_file)

    np.random.seed(turn_file_config.seed)

    if turn_file_config.sql_filter:
        df = (
            pl.SQLContext(frames=dict(frame=df))
            .execute(
                turn_file_config.sql_filter,
            )
            .collect()
        )

    st = datetime.strptime(turn_file_config.start_time, "%Y-%m-%d %H:%M:%S")
    # update the timezone with the one in the dataframe
    st = st.replace(tzinfo=df[turn_file_config.time_column][0].tzinfo)

    if turn_file_config.end_time:
        et = datetime.strptime(turn_file_config.end_time, "%Y-%m-%d %H:%M:%S")
        # update the timezone with the one in the dataframe
        et = et.replace(tzinfo=df[turn_file_config.time_column][0].tzinfo)
    else:
        et = df[turn_file_config.time_column].max()

    df = (
        df.sort(turn_file_config.time_column)
        .groupby_dynamic(
            index_column=turn_file_config.time_column,
            every=f"{int(turn_file_config.agg_interval)}s",
            by=["to", "from"],
        )
        .agg(pl.col("volume").sum())
        .with_columns(
            # probabilisticall round to an integer
            pl.col("volume")
            .map(
                lambda x: np.floor(x.to_numpy())
                + 1
                * (
                    np.random.uniform(0, 1, size=(len(x)))
                    < np.remainder(x.to_numpy(), 1)
                )
            )
            .cast(int)
        )
        .filter(
            (pl.col(turn_file_config.time_column) >= st)
            & (pl.col(turn_file_config.time_column) <= et)
        )
        .sort(turn_file_config.time_column)
        .with_columns(
            (
                (pl.col(turn_file_config.time_column) - st).dt.total_milliseconds()
                / 1e3
            ).alias("begin"),
            (
                (
                    pl.col(turn_file_config.time_column).shift(-1).forward_fill() - st
                ).dt.total_milliseconds()
                / 1e3
            )
            .over(["to", "from"])
            .alias("end"),
        )
        .filter(pl.col("begin") < pl.col("end"))
        .with_columns(
            pl.when(pl.col("to").is_null())
            .then(
                pl.format(
                    """\t\t<edge id="{}" entered="{}" />\n""",
                    pl.col("from"),
                    pl.col("volume").cast(str),
                )
            )
            .otherwise(
                pl.format(
                    """\t\t<edgeRelation from="{}" to="{}" count="{}" />\n""",
                    pl.col("from"),
                    pl.col("to"),
                    pl.col("volume").cast(str),
                )
            )
            .alias("write_str")
        )
    )

    def write_lines(_df: pl.DataFrame, f_) -> None:
        f_.write(
            f"""\t<interval id="interval_{_df['begin'][0]!s}" begin="{_df['begin'][0]!s}" end="{_df['end'][0]!s}" >\n"""
        )
        f_.write("".join(_df["write_str"].to_list()))

        f_.write("""\t</interval>\n""")

        return _df

    with open(turn_file_config.output_file, "w") as f_:
        f_.write("""<additional>\n""")

        df.groupby("begin", "end", maintain_order=True).apply(
            lambda _df: write_lines(_df, f_)
        )

        f_.write("""</additional>\n""")


def call_route_sampler(
    route_sampler_config: RouteSamplerConfig,
    config: DictConfig,
) -> None:
    routeSampler = import_sumo_tools_module("routeSampler")

    routeSampler.main(
        routeSampler.get_options(
            [
                "-r",
                str(route_sampler_config.random_route_file),
                "-t",
                str(route_sampler_config.turn_file),
                "-o",
                str(route_sampler_config.output_file),
                "--seed",
                str(int(route_sampler_config.seed)),
                *route_sampler_config.additional_args,
            ]
        )
    )


def call_random_trips(
    config: RandomTripsConfig,
    *args,
    **kwargs,
) -> Path:
    randomTrips = import_sumo_tools_module("randomTrips")

    net_file = config.net_file
    output_file = Path(config.output_file)
    seed = int(config.seed)

    randomTrips.main(
        randomTrips.get_options(
            [
                "-n",
                str(net_file),
                "-r",
                str(output_file),
                "-o",
                str(output_file.parent / "routes.add.xml"),
                "--validate",
                "--seed",
                str(seed),
                *config.additional_args,
            ]
        )
    )

    return output_file
