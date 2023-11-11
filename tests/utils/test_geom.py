from sumo_pipelines.sumo_pipelines_rs import is_inside_sm_parallel_py


def test_is_inside_sm():
    points = [(1.0, 1.0)]
    polygon = [(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]
    result = is_inside_sm_parallel_py(points, polygon)
    assert result[0] == 1
