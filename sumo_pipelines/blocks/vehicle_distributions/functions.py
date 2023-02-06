from omegaconf import DictConfig

from .config import CFTableConfig

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
        samples = DUMB_OBJECT_STORE[cf_config.table]
    else:
        samples = pd.read_csv(
            cf_config.table,
        )
        DUMB_OBJECT_STORE[cf_config.table] = samples

    samples = samples.rename(columns=cf_config.cf_params)

    # add the vehicle type
    vehicles = []
    i = 0

    def create_veh_type(row: pd.Series) -> None:
        nonlocal i
        vehicles.append(
            f'\t<vType id="{cf_config.vehicle_distribution_name}_{i}" '
            # + " ".join(['{}="{}"'.format(k, str(v)) for k, v in row.items()])
            # + " "
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
