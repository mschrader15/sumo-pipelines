from pathlib import Path

import numba
import numpy as np
from numba import jit, njit
from sumolib.shapes import polygon


def get_polygon(
    polygon_file,
):
    if polygon_file:
        assert Path(polygon_file).exists(), "The polygon file does not exist"

        polygons = polygon.read(polygon_file)
        assert len(polygons) == 1, "The polygon file should only contain one polygon"

        return polygons[0].shape
    return None


@jit(nopython=True)
def is_inside_sm(polygon, point):
    length = len(polygon) - 1
    dy2 = point[1] - polygon[0][1]
    intersections = 0
    ii = 0
    jj = 1

    while ii < length:
        dy = dy2
        dy2 = point[1] - polygon[jj][1]

        # consider only lines which are not completely above/bellow/right from the point
        if dy * dy2 <= 0.0 and (
            point[0] >= polygon[ii][0] or point[0] >= polygon[jj][0]
        ):
            # non-horizontal line
            if dy < 0 or dy2 < 0:
                F = dy * (polygon[jj][0] - polygon[ii][0]) / (dy - dy2) + polygon[ii][0]

                if (
                    point[0] > F
                ):  # if line is left from the point - the ray moving towards left, will intersect it
                    intersections += 1
                elif point[0] == F:  # point on line
                    return 2

            # point on upper peak (dy2=dx2=0) or horizontal line (dy=dy2=0 and dx*dx2<=0)
            elif dy2 == 0 and (
                point[0] == polygon[jj][0]
                or (
                    dy == 0
                    and (point[0] - polygon[ii][0]) * (point[0] - polygon[jj][0]) <= 0
                )
            ):
                return 2

        ii = jj
        jj += 1

    # print 'intersections =', intersections
    return intersections & 1


@njit(parallel=True)
def is_inside_sm_parallel(points, polygon):
    ln = len(points)
    D = np.empty(ln, dtype=numba.boolean)
    for i in numba.prange(ln):
        D[i] = is_inside_sm(polygon, points[i])
    return D
