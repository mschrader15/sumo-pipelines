from dataclasses import dataclass

from omegaconf import OmegaConf

from sumo_pipelines.blocks.producers.config import (
    SobolItem,
    SobolSequenceConfig,
)
from sumo_pipelines.blocks.producers.functions import (
    generate_sobol_sequence,
)
from sumo_pipelines.config import MetaData


def test_sobol() -> None:
    c = SobolSequenceConfig(
        save_path="./sequence.csv",
        N=64,
        calc_second_order=True,
        params={
            "accel": SobolItem(bounds=[1, 2.6]),
            "decel": SobolItem(bounds=[2, 4]),
        },
    )

    @dataclass
    class SimpleParent:
        config: SobolSequenceConfig
        Metadata: MetaData

    parent = OmegaConf.structured(
        SimpleParent(
            config=c,
            Metadata=MetaData(
                name="",
                author="",
                output="",
                run_id="",
                cwd="",
                simulation_root="",
                random_seed=42,
            ),
        )
    )

    _ = list(generate_sobol_sequence(c, parent, dotpath="config"))
