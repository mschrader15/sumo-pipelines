from dataclasses import dataclass, field


@dataclass
class RouteSamplerConfig:
    turn_file: str
    output_file: str
    random_route_file: str
    additional_args: list
    seed: int = field(default=42)
    
