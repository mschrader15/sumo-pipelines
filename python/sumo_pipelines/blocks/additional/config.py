from dataclasses import dataclass, field


@dataclass
class WayPointConfig:
    trajectory_file: str
    waypoint_file: str
    veh_type: str
    route: str
