from dataclasses import dataclass
from typing import Tuple


@dataclass
class RandomSeedConfig:
    seed: int
    range: Tuple[int, int]
