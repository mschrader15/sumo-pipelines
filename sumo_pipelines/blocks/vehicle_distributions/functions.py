from typing import List

import numpy as np
import pandas as pd
import polars as pl
import scipy.stats as stats
from omegaconf import DictConfig

from sumo_pipelines.utils.config_helpers import config_wrapper

from .config import (
    CFAddParamsConfig,
    CFTableConfig,
    MergeVehDistributionsConfig,
    MultiTypeCFConfig,
    ParamConfig,
    SampledSimpleCFConfig,
    SimpleCFConfig,
)

pandas_not_installed = False

DUMB_OBJECT_STORE = {}


def _create_distribution_table(
    df: pl.DataFrame, distribution_name: str, write_columns: List[str], save_path: str
) -> None:
    write_text = (
        df.with_row_index(name="id")
        .with_columns(pl.col(pl.FLOAT_DTYPES).round(3))
        .select(
            pl.concat_str(
                (
                    pl.format(
                        f'\t\t<vType id="{distribution_name}_' + '{}"',
                        pl.col("id"),
                    ),
                    *(
                        pl.format('{}="{}"', pl.lit(col).alias(col), pl.col(col))
                        for col in write_columns
                    ),
                    pl.lit("/>"),
                ),
                separator=" ",
            )
            .str.concat("\n")
            .alias("out")
        )["out"][0]
    )

    with open(save_path, "w") as f:
        f.write(f'<vTypeDistribution id="{distribution_name}" >\n')
        f.write(write_text)
        f.write("</vTypeDistribution>")


@config_wrapper
def create_distribution_table(
    cf_config: CFTableConfig,
    config: DictConfig,
    *args,
    **kwargs,
) -> None:
    if cf_config.table in DUMB_OBJECT_STORE:
        samples = DUMB_OBJECT_STORE[cf_config.table].clone()
    else:
        samples = pl.read_parquet(
            cf_config.table,
        )

        if cf_config.write_parameters:
            samples = samples.select(cf_config.write_parameters)

        DUMB_OBJECT_STORE[cf_config.table] = samples

    sampled_df = samples.sample(
        n=cf_config.num_samples, seed=cf_config.seed, with_replacement=True
    )

    _create_distribution_table(
        sampled_df,
        cf_config.vehicle_distribution_name,
        write_columns=cf_config.write_parameters,
        save_path=cf_config.save_path,
    )


@config_wrapper
def create_independent_distribution_pandas(
    cf_config: CFTableConfig,
    config: DictConfig,
) -> None:
    if pandas_not_installed:
        raise ImportError(
            "pandas is not installed. Please install it to use this block."
        )

    if cf_config.table in DUMB_OBJECT_STORE:
        samples = DUMB_OBJECT_STORE[cf_config.table].copy()
    else:
        samples = pd.read_csv(
            cf_config.table,
        )
        DUMB_OBJECT_STORE[cf_config.table] = samples

    samples = samples.rename(columns=cf_config.cf_params)
    # add the vehicle type

    sampled_params = {
        col: samples[col].sample(cf_config.num_samples, random_state=cf_config.seed)
        for col in samples.columns
    }

    vehicles = [
        (
            (
                f'\t<vType id="{cf_config.vehicle_distribution_name}_{i}" '
                + " ".join([f'{k}="{v[i]!s}"' for k, v in sampled_params.items()])
            )
            + " "
        )
        + " ".join([f'{k}="{v!s}"' for k, v in cf_config.additional_params.items()])
        + "/>"
        for i in range(cf_config.num_samples)
    ]

    with open(cf_config.save_path, "w") as f:
        f.write(f'<vTypeDistribution id="{cf_config.vehicle_distribution_name}" >\n')
        for v in vehicles:
            f.write(v + "\n")
        f.write("</vTypeDistribution>")


@config_wrapper
def create_simple_distribution(
    cf_config: SimpleCFConfig,
    config: DictConfig,
) -> None:
    i = 0
    vehicles = [
        f'\t<vType id="{cf_config.vehicle_distribution_name}_{i}" '
        + " ".join([f'{k}="{v!s}"' for k, v in cf_config.cf_params.items()])
        + "/>"
    ]
    with open(cf_config.save_path, "w") as f:
        f.write(f'<vTypeDistribution id="{cf_config.vehicle_distribution_name}" >\n')
        for v in vehicles:
            f.write(v + "\n")
        f.write("</vTypeDistribution>")


def _parse_num_samples(num_samples: str) -> int:
    import re

    if isinstance(num_samples, (int, float)):
        return int(num_samples)

    if "uniform" in num_samples:
        raise NotImplementedError("Uniform sampling not implemented")
        # y = eval(num_samples.strip("uniform"))
        # return int(random.uniform(y[0], y[1]))

    if " - " in num_samples:
        # use regex to get the numbers
        matches = re.findall(r"\d+", num_samples)
        return int(matches[0]) - int(matches[1])


@config_wrapper
def create_simple_sampled_distribution(
    cf_config: SampledSimpleCFConfig, config: DictConfig
):
    from sumolib.vehicletype import CreateVehTypeDistribution, VehAttribute

    dist_creator = CreateVehTypeDistribution(
        int(cf_config.seed),
        _parse_num_samples(cf_config.num_samples),
        cf_config.vehicle_distribution_name,
        decimal_places=cf_config.decimal_places,
    )

    for k, v in cf_config.cf_params.items():
        if v.is_attr:
            dist_creator.add_attribute(VehAttribute(name=k, attribute_value=str(v.val)))
        else:
            dist_creator.add_attribute(
                VehAttribute(
                    name=k,
                    distribution=v.distribution,
                    distribution_params=v.params,
                    bounds=v.bounds,
                )
            )

    # open an xml dom for writing at the save path
    dist_creator.to_xml(
        cf_config.save_path,
    )


def _sample_dist(param: ParamConfig, n: int, seed: int = 42) -> np.array:
    dist = getattr(
        stats,
        param.distribution,
    )(**param.params)

    # first fast pass
    vals = dist.rvs(n, random_state=seed)
    if param.bounds:
        param.bounds = [float(b) for b in param.bounds]

        def get_ob() -> np.array:
            return (vals < param.bounds[0]) | (vals > param.bounds[1])

        ob = get_ob()

        # could also do this by sampling 10x the target or whatever
        i = 0
        while np.any(ob) and (i < 1000):
            vals[ob] = dist.rvs(np.sum(ob))
            ob = get_ob()
            i += 1

        if i == 1000:
            vals[vals > param.bounds[1]] = param.bounds[1]
            vals[vals < param.bounds[0]] = param.bounds[0]

    return vals


@config_wrapper
def create_simple_sampled_distribution_scipy(
    cf_config: SampledSimpleCFConfig, *args, **kwargs
) -> None:
    # np.random.seed(seed=int(cf_config.seed))

    param_dict = {}
    n = cf_config.num_samples
    select_strs = []
    for k, v in cf_config.cf_params.items():
        if v.is_attr:
            param_dict[k] = [
                str(v.val),
            ] * n
        else:
            param_dict[k] = _sample_dist(v, n, seed=int(cf_config.seed))

        if v.transform:
            select_strs.append(v.transform)
        else:
            select_strs.append(f"{k} as {k}")

    sampled_df = pl.DataFrame(param_dict).with_columns(
        pl.col(pl.FLOAT_DTYPES).round(cf_config.decimal_places),
        pl.col(pl.INTEGER_DTYPES).cast(float),  # this saves wierdness w/ sql
    )

    transformed_df = (
        pl.SQLContext({"frame": sampled_df})
        .execute(f"SELECT {','.join(select_strs)} FROM frame")
        .collect()
    )

    _create_distribution_table(
        transformed_df,
        distribution_name=cf_config.vehicle_distribution_name,
        write_columns=list(param_dict.keys()),
        save_path=cf_config.save_path,
    )


@config_wrapper
def create_multi_type_distribution(
    cf_config: MultiTypeCFConfig,
    *args,
    **kwargs,
) -> None:
    for config in cf_config.configs:
        try:
            create_simple_sampled_distribution(
                config,
                *args,
            )
        except Exception as e:
            print(f"Error creating distribution {config.vehicle_distribution_name}")
            raise e
        # if isinstance(config, SampledSimpleCFConfig):
        #     create_simple_sampled_distribution(config, *args, **kwargs)
        # elif isinstance(config, SimpleCFConfig):
        #     create_simple_distribution(config, *args, **kwargs)
        # elif isinstance(config, CFTableConfig):
        #     create_distribution_pandas(config, *args, **kwargs)
        # else:
        #     raise ValueError(f"Unknown config type {type(config)}")

    # merge the distributions
    merge_veh_distributions(
        MergeVehDistributionsConfig(
            files=[config.save_path for config in cf_config.configs],
            distribution_name=cf_config.distribution_name,
            output_path=cf_config.save_path,
        )
    )


@config_wrapper
def merge_veh_distributions(
    config: MergeVehDistributionsConfig,
    *args,
    **kwargs,
) -> None:
    # from lxml
    from lxml import etree as ET

    # make a new XML doc
    doc = ET.ElementTree(ET.fromstring("<additional/>"))
    root = doc.getroot()
    dist = ET.Element("vTypeDistribution", attrib={"id": config.distribution_name})

    all_vtypes = []
    for i, file_ in enumerate(config.files):
        tree = ET.parse(file_)
        for vtype in tree.findall(".//vType"):
            vtype.attrib["id"] = f"{vtype.attrib['id']}_{i}"
            all_vtypes.append(vtype)
    dist.extend(all_vtypes)
    root.append(dist)
    # save the file
    doc.write(config.output_path, pretty_print=True)


def _custom_to_xml(
    df: pd.DataFrame,
    file_path: str,
    elem_cols: List[str],
):
    import lxml.etree as etree

    data = df.iloc[0].to_dict()
    df = df.iloc[1:].copy()

    attr_cols = list(df.columns.difference(elem_cols))
    elem_cols = elem_cols

    # these are constant
    root_name = "vTypeDistribution"
    dist_name = data["id"]
    row_name = "vType"
    param_name = "param"

    root = etree.Element(root_name)
    root.set("id", dist_name)
    for _, row in df.iterrows():
        row_elem = etree.SubElement(root, row_name)
        for attr in attr_cols:
            if not pd.isna(row[attr]):
                row_elem.set(attr, str(row[attr]))
        for elem in elem_cols:
            if not pd.isna(row[elem]):
                elem_elem = etree.SubElement(row_elem, param_name)
                # elem_elem.text = str(row[elem])
                # for col in elem_cols:
                elem_elem.set("key", elem)
                elem_elem.set("value", str(row[elem]))

    tree = etree.ElementTree(root)
    tree.write(file_path, pretty_print=True, xml_declaration=True, encoding="utf-8")


@config_wrapper
def update_veh_distribution(
    cf_config: CFAddParamsConfig,
    config: DictConfig,
) -> None:
    # read in the vtype distribution

    df = pd.read_xml(cf_config.input_file, xpath=".//*")

    for param in cf_config.params:
        df[param.name] = None
        _df = df
        for filt in param.filters:
            filt.value = (
                f'"{filt.value}"' if isinstance(filt.value, str) else filt.value
            )
            _df = _df.query(f"{filt.param} == {filt.value}")

        ind = _df.sample(
            frac=param.percent,
        ).index
        df.loc[ind, param.name] = param.value

    _custom_to_xml(df, cf_config.save_path, [c.name for c in cf_config.params])
