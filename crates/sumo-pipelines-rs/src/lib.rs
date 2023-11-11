pub mod geom;
pub mod xml_parsing;

use geom::utils::{is_inside_sm, is_inside_sm_parallel};
use pyo3::{pyfunction, pymodule, types::PyModule, wrap_pyfunction, PyResult, Python};
use xml_parsing::emissions::{parse_xml_raw, socket_emissions};

#[pyfunction]
fn parse_emissions_xml(file_path: &str, output_path: &str, output_base_name: &str) -> PyResult<()> {
    parse_xml_raw(file_path, output_path, output_base_name).unwrap();
    Ok(())
}

#[pyfunction]
fn parse_socket_emissions(
    socket_address: &str,
    output_path: &str,
    output_base_name: &str,
) -> PyResult<()> {
    socket_emissions(socket_address, output_path, output_base_name).unwrap();
    Ok(())
}

#[pyfunction]
fn is_inside_sm_py(polygon: Vec<(f64, f64)>, point: (f64, f64)) -> PyResult<i32> {
    Ok(is_inside_sm(&polygon, &point))
}

#[pyfunction]
fn is_inside_sm_parallel_py(
    points: Vec<(f64, f64)>,
    polygon: Vec<(f64, f64)>,
) -> PyResult<Vec<bool>> {
    Ok(is_inside_sm_parallel(points, polygon))
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "sumo_pipelines_rs")]
fn sumo_pipelines_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_emissions_xml, m)?)?;
    m.add_function(wrap_pyfunction!(parse_socket_emissions, m)?)?;
    m.add_function(wrap_pyfunction!(is_inside_sm_py, m)?)?;
    m.add_function(wrap_pyfunction!(is_inside_sm_parallel_py, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::{types::IntoPyDict, PyAny};

    #[test]
    fn test_is_inside_sm_py() {
        pyo3::prepare_freethreaded_python();

        let command = format!(
            "point = (1.0, 1.0)                                                                                                                                                                                                                           
            polygon = [(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)]                                                                                                                                                                                    
            my_module.is_inside_sm_py(polygon, point)"
        );

        Python::with_gil(|py| {
            let sumo_pipelines_rs = PyModule::new(py, "sumo_pipelines_rs").unwrap();
            let locals = [("sumo_pipelines_rs", sumo_pipelines_rs)].into_py_dict(py);
            let result: PyResult<&PyAny> = py.eval(&command, Some(locals), None);
            // print the result
            println!("{:?}", result);
            assert!(result.is_ok());
        });
    }
}

// #[cfg(test)]
// mod pytests {
//     use super::*;
//     use pyo3::{types::IntoPyDict, PyAny};

//     #[test]
//     fn test_parse_emissions_xml_to_df_py() {
//         let file_path = "tests/test_data/emissions.xml";
//         let output_path = "tests/test_data/emissions.parquet2";
//         let command = format!(
//             "sumo_pipelines.sumo_pipelines_rs.parse_emissions_xml(\"{}\", \"{}\")",
//             file_path,
//             output_path
//         );

//         pyo3::prepare_freethreaded_python();

//         Python::with_gil(|py| {
//             let sumo_pipelines_rs = PyModule::new(py, "sumo_pipelines_rs").unwrap();
//             let locals = [("sumo_pipelines_rs", sumo_pipelines_rs)].into_py_dict(py);
//             let result: PyResult<&PyAny> = py
//                 .eval(
//                     &command,
//                     Some(locals),
//                     None,
//                 );
//             // print the result
//             println!("{:?}", result);
//             assert!(result.is_ok());
//         });
//     }
// }
