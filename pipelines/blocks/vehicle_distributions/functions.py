from .config import CFTableConfig

try:
    import pandas as pd
    pandas_not_installed = False
except ImportError:
    pandas_not_installed = True
    



def create_distribution_pandas(
    cf_config: CFTableConfig,
) -> None:

    if pandas_not_installed:
        raise ImportError("pandas is not installed. Please install it to use this block.")

    samples = cf_config.sample_parameter(n=cf_config.num_samples, )

    # remap the parameters to the cf model
    samples = samples.rename(columns=cf_config.cf_params)
    # add the cf model
    samples["cf_model"] = cf_config.cf_model
    # add the vehicle type
    samples["type"] = cf_config.veh_type
    vehicles = []
    i = 0
    
    def create_veh_type(row: pd.Series) -> None:
        nonlocal i
        vehicles.append(
            f'\t<vType id="{cf_config.vehicle_distribution_name}_{i}"' + ' '.join(['="{}"'.format(k, v) for k, v in row.items()]) + '/>'
        )
        i += 1

    samples.apply(create_veh_type, axis=1)

    with open(cf_config.file_path, "w") as f:
        f.write(f'<vTypeDistribution id="{cf_config.vehicle_distribution_name}" >\n')
        for v in vehicles:
            f.write(v + '\n')
        f.write('</vTypeDistribution>')