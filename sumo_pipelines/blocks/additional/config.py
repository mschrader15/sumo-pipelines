from dataclasses import dataclass


@dataclass
class WayPointConfig:
    trajectory_file: str
    waypoint_file: str
    veh_type: str
    route: str
