from typing import Generic, TypeVar

T_co = TypeVar("T_co", covariant=True)


class PipelineConfig(Generic[T_co]):
    """Pipeline Config stub"""

    ...


class OptimizationConfig(Generic[T_co]):
    """Optimization Config stub"""

    ...
