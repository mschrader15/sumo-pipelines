from omegaconf import DictConfig

from sumo_pipelines.utils.nema_utils import NEMALight

from .config import NEMAUpdateConfig


def update_nema(
    nema_config: NEMAUpdateConfig,
    parent_config: DictConfig,
    *args,
    **kwargs,
) -> None:
    """
    This function updates a NEMA file with the given configuration.

    Args:
        nema_config (NEMAUpdateConfig): The configuration for the NEMA update.
        parent_config (DictConfig): The parent configuration.
        nema_file (str): The path to the NEMA file to update.
        out_file (str): The path to the output file.
    """

    for nema_conf in nema_config.tls:
        nema = NEMALight.from_xml(
            nema_conf.in_file,
            nema_conf.id,
            nema_conf.program_id,
        )

        if nema_conf.splits:
            nema.update_coordinate_splits(
                dict(
                    zip(
                        [split.phase for split in nema_conf.splits],
                        [split.split for split in nema_conf.splits],
                    )
                )
            )

        if nema_conf.offset:
            nema.update_offset(nema_conf.offset)

        for phase_update in nema_conf.phase_updates:
            nema.update_phase(
                phase_update.phase, **{phase_update.key: phase_update.val}
            )

        with open(nema_conf.out_file, "w") as f:
            f.write(nema.to_xml())
