from omegaconf import DictConfig

from .config import CFTableConfig, MergeVehDistributions, SimpleCFConfig, SampledSimpleCFConfig

try:
    import pandas as pd

    pandas_not_installed = False
except ImportError:
    pandas_not_installed = True


DUMB_OBJECT_STORE = {}


def create_distribution_pandas(
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

    samples = samples.rename(columns=cf_config.cf_params).sample(
        cf_config.num_samples,
        random_state=cf_config.seed,
        replace=True
    )
    # add the vehicle type
    vehicles = []
    i = 0

    # add the vehicle type
    vehicles = []
    i = 0

    def create_veh_type(row: pd.Series) -> None:
        nonlocal i
        vehicles.append(
            f'\t<vType id="{cf_config.vehicle_distribution_name}_{i}" '
            + " ".join(['{}="{}"'.format(k, str(v)) for k, v in row.items()])
            + " "
            + " ".join(['{}="{}"'.format(k, str(v)) for k, v in cf_config.additional_params.items()])
            + "/>"
        )
        i += 1

    samples.apply(create_veh_type, axis=1)

    with open(cf_config.save_path, "w") as f:
        f.write(f'<vTypeDistribution id="{cf_config.vehicle_distribution_name}" >\n')
        for v in vehicles:
            f.write(v + "\n")
        f.write("</vTypeDistribution>")



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
        col: samples[col].sample(
           cf_config.num_samples,
            random_state=cf_config.seed
        ) for col in samples.columns
    }

    vehicles = [
        (
            (
                f'\t<vType id="{cf_config.vehicle_distribution_name}_{i}" '
                + " ".join(
                    [f'{k}="{str(v[i])}"' for k, v in sampled_params.items()]
                )
            )
            + " "
        )
        + " ".join(
            [f'{k}="{str(v)}"' for k, v in cf_config.additional_params.items()]
        )
        + "/>"
        for i in range(cf_config.num_samples)
    ]

    with open(cf_config.save_path, "w") as f:
        f.write(f'<vTypeDistribution id="{cf_config.vehicle_distribution_name}" >\n')
        for v in vehicles:
            f.write(v + "\n")
        f.write("</vTypeDistribution>")



def create_simple_distribution(
    cf_config: SimpleCFConfig,
    config: DictConfig,
) -> None:
    
    i = 0
    vehicles = [
        f'\t<vType id="{cf_config.vehicle_distribution_name}_{i}" '
        + " ".join(
            [f'{k}="{str(v)}"' for k, v in cf_config.cf_params.items()]
        )
        + "/>"
    ]
    with open(cf_config.save_path, "w") as f:
        f.write(f'<vTypeDistribution id="{cf_config.vehicle_distribution_name}" >\n')
        for v in vehicles:
            f.write(v + "\n")
        f.write("</vTypeDistribution>")



def create_simple_sampled_distribution(cf_config: SampledSimpleCFConfig, config: DictConfig):
    from sumolib.vehicletype import CreateVehTypeDistribution, VehAttribute


    dist_creator = CreateVehTypeDistribution(
        cf_config.seed,
        cf_config.num_samples,
        cf_config.vehicle_distribution_name,
        decimal_places=cf_config.decimal_places,
    )

    for k, v in cf_config.cf_params.items():
        if v.is_attr:
            dist_creator.add_attribute(
                VehAttribute(
                    name=k,
                    attribute_value=str(v.val)
                )
            )
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
    
    

def merge_veh_distributions(
    config: MergeVehDistributions,
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
    for file_ in config.files:
        tree = ET.parse(file_)
        all_vtypes.extend(tree.findall(".//vTypeDistribution/*"))
    dist.extend(all_vtypes)
    root.append(dist)
    
    # save the file
    doc.write(config.output_path, pretty_print=True)
    
    
def update_veh_distribution(
    cf_config: MergeVehDistributions,
    config: DictConfig,
) -> None:
    # read in the vtype distribution
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas is not installed. Please install it to use this block.")
    # using pandas for this is :shrug: but I have it as a dependency already 
    # so whatever
    
    df = pd.read_xml(
        cf_config.save_path,
        xpath="vTypeDistribution/vType",
    )
    
    # update the 
    
    