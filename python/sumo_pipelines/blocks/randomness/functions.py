from .config import RandomSeedConfig
import random



def set_random_seed(config: RandomSeedConfig, *args, **kwargs) -> None:
    config.seed = random.randint(*config.range)
    
