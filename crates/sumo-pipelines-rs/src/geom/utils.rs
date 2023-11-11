extern crate rayon;
use rayon::prelude::*;

pub fn is_inside_sm(polygon: &Vec<(f64, f64)>, point: &(f64, f64)) -> i32 {
    // from https://stackoverflow.com/a/66189882
    let length = polygon.len() - 1;
    let mut dy2 = point.1 - polygon[0].1;
    let mut intersections = 0;
    let mut ii = 0;
    let mut jj = 1;

    while ii < length {
        let dy = dy2;
        dy2 = point.1 - polygon[jj].1;

        if dy * dy2 <= 0.0 && (point.0 >= polygon[ii].0 || point.0 >= polygon[jj].0) {
            if dy < 0.0 || dy2 < 0.0 {
                let f = dy * (polygon[jj].0 - polygon[ii].0) / (dy - dy2) + polygon[ii].0;

                if point.0 > f {
                    intersections += 1;
                } else if (point.0 - f).abs() < std::f64::EPSILON {
                    return 2;
                }
            } else if dy2.abs() < std::f64::EPSILON
                && (point.0 == polygon[jj].0
                    || dy.abs() < std::f64::EPSILON
                        && (point.0 - polygon[ii].0) * (point.0 - polygon[jj].0) <= 0.0)
            {
                return 2;
            }
        }

        ii = jj;
        jj += 1;
    }

    intersections & 1
}

pub fn is_inside_sm_parallel(points: Vec<(f64, f64)>, polygon: Vec<(f64, f64)>) -> Vec<bool> {
    points
        .par_iter()
        .map(|&point| is_inside_sm(&polygon, &point) != 0)
        .collect()
}


