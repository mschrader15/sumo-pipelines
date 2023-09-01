use arrow2::{
    array::{Array, Float64Array, Utf8Array},
    chunk::Chunk,
    // error::Result,
    datatypes::{DataType, Field, Schema},
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};
use core::f64;
use std::{
    fs::{self},
    io::{BufRead, BufReader, Read},
    net::{TcpListener, TcpStream},
    path::Path,
};

const MAX_ROWS_PER_FILE: usize = 500_000;

#[derive(Debug, Clone, Default)]
pub struct TimeItem {
    timestep: f64,
    id: String,
    eclass: String,
    co2: f64,
    co: f64,
    hc: f64,
    nox: f64,
    pmx: f64,
    fuel: f64,
    electricity: f64,
    noise: f64,
    route: String,
    type_: String,
    waiting: f64,
    lane: String,
    pos: f64,
    speed: f64,
    angle: f64,
    x: f64,
    y: f64,
}

/// Stores the data in a columnar format, as it comes in from the radar via rows
#[allow(non_snake_case)]
#[derive(Debug, Clone)]
pub struct ColumnStore {
    timestep: Vec<f64>,
    id: Vec<String>,
    eclass: Vec<String>,
    co2: Vec<f64>,
    co: Vec<f64>,
    hc: Vec<f64>,
    nox: Vec<f64>,
    pmx: Vec<f64>,
    fuel: Vec<f64>,
    electricity: Vec<f64>,
    noise: Vec<f64>,
    route: Vec<String>,
    type_: Vec<String>,
    waiting: Vec<f64>,
    lane: Vec<String>,
    pos: Vec<f64>,
    speed: Vec<f64>,
    angle: Vec<f64>,
    x: Vec<f64>,
    y: Vec<f64>,
    pub schema: Schema,
    pub encodings: Vec<Vec<Encoding>>,
}

impl Default for ColumnStore {
    fn default() -> Self {
        // IMPORTANT: The order of the fields must match the order of the fields in the schema
        let schema = Schema::from(vec![
            Field::new("timestep", DataType::Float64, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("eclass", DataType::Utf8, false),
            Field::new("co2", DataType::Float64, false),
            Field::new("co", DataType::Float64, false),
            Field::new("hc", DataType::Float64, false),
            Field::new("nox", DataType::Float64, false),
            Field::new("pmx", DataType::Float64, false),
            Field::new("fuel", DataType::Float64, false),
            Field::new("electricity", DataType::Float64, false),
            Field::new("noise", DataType::Float64, false),
            Field::new("route", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("waiting", DataType::Float64, false),
            Field::new("lane", DataType::Utf8, false),
            Field::new("pos", DataType::Float64, false),
            Field::new("speed", DataType::Float64, false),
            Field::new("angle", DataType::Float64, false),
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);

        let encodings: Vec<Vec<Encoding>> = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();

        ColumnStore {
            timestep: Vec::with_capacity(MAX_ROWS_PER_FILE),
            id: Vec::with_capacity(MAX_ROWS_PER_FILE),
            eclass: Vec::with_capacity(MAX_ROWS_PER_FILE),
            co2: Vec::with_capacity(MAX_ROWS_PER_FILE),
            co: Vec::with_capacity(MAX_ROWS_PER_FILE),
            hc: Vec::with_capacity(MAX_ROWS_PER_FILE),
            nox: Vec::with_capacity(MAX_ROWS_PER_FILE),
            pmx: Vec::with_capacity(MAX_ROWS_PER_FILE),
            fuel: Vec::with_capacity(MAX_ROWS_PER_FILE),
            electricity: Vec::with_capacity(MAX_ROWS_PER_FILE),
            noise: Vec::with_capacity(MAX_ROWS_PER_FILE),
            route: Vec::with_capacity(MAX_ROWS_PER_FILE),
            type_: Vec::with_capacity(MAX_ROWS_PER_FILE),
            waiting: Vec::with_capacity(MAX_ROWS_PER_FILE),
            lane: Vec::with_capacity(MAX_ROWS_PER_FILE),
            pos: Vec::with_capacity(MAX_ROWS_PER_FILE),
            speed: Vec::with_capacity(MAX_ROWS_PER_FILE),
            angle: Vec::with_capacity(MAX_ROWS_PER_FILE),
            x: Vec::with_capacity(MAX_ROWS_PER_FILE),
            y: Vec::with_capacity(MAX_ROWS_PER_FILE),
            schema,
            encodings,
        }
    }
}

impl ColumnStore {
    pub fn ready_2_write(&self) -> bool {
        self.timestep.len() >= MAX_ROWS_PER_FILE
    }

    pub fn append_columns(&mut self, data: &Vec<TimeItem>) {
        // build the parquet columns, create a column for each field in the schema.
        //  should be automated using the schema.
        data.iter().for_each(|d| {
            self.timestep.push(d.timestep);
            self.id.push(d.id.clone());
            self.eclass.push(d.eclass.clone());
            self.co2.push(d.co2);
            self.co.push(d.co);
            self.hc.push(d.hc);
            self.nox.push(d.nox);
            self.pmx.push(d.pmx);
            self.fuel.push(d.fuel);
            self.electricity.push(d.electricity);
            self.noise.push(d.noise);
            self.route.push(d.route.clone());
            self.type_.push(d.type_.clone());
            self.waiting.push(d.waiting);
            self.lane.push(d.lane.clone());
            self.pos.push(d.pos);
            self.speed.push(d.speed);
            self.angle.push(d.angle);
            self.x.push(d.x);
            self.y.push(d.y);
        });
    }

    /// Convert the column store into a record batch for the parquet writer
    pub fn build_arrays(&self) -> arrow2::error::Result<Chunk<Box<dyn Array>>> {
        // this feels horrible, but I don't know how to do it better
        let chunk = Chunk::try_new(vec![
            Float64Array::from_vec(self.timestep.clone()).boxed(),
            Utf8Array::<i32>::from_iter(self.id.iter().map(|s| Some(s))).boxed(),
            Utf8Array::<i32>::from_iter(self.eclass.iter().map(|s| Some(s))).boxed(),
            Float64Array::from_vec(self.co2.clone()).boxed(),
            Float64Array::from_vec(self.co.clone()).boxed(),
            Float64Array::from_vec(self.hc.clone()).boxed(),
            Float64Array::from_vec(self.nox.clone()).boxed(),
            Float64Array::from_vec(self.pmx.clone()).boxed(),
            Float64Array::from_vec(self.fuel.clone()).boxed(),
            Float64Array::from_vec(self.electricity.clone()).boxed(),
            Float64Array::from_vec(self.noise.clone()).boxed(),
            Utf8Array::<i32>::from_iter(self.route.iter().map(|s| Some(s))).boxed(),
            Utf8Array::<i32>::from_iter(self.type_.iter().map(|s| Some(s))).boxed(),
            Float64Array::from_vec(self.waiting.clone()).boxed(),
            Utf8Array::<i32>::from_iter(self.lane.iter().map(|s| Some(s))).boxed(),
            Float64Array::from_vec(self.pos.clone()).boxed(),
            Float64Array::from_vec(self.speed.clone()).boxed(),
            Float64Array::from_vec(self.angle.clone()).boxed(),
            Float64Array::from_vec(self.x.clone()).boxed(),
            Float64Array::from_vec(self.y.clone()).boxed(),
        ])?;
        Ok(chunk)
    }
}

/// A rust implementation of a parquet file writer.
/// Rotates the file every MAX_ROWS_PER_FILE rows.
#[derive(Debug, Clone)]
pub struct CustomFileWriter<'a> {
    file_name: Option<String>,
    base_name: &'a str,
    output_path: &'a Path,
    column_store: ColumnStore,
    write_options: WriteOptions,
    file_num: usize,
}

impl CustomFileWriter<'_> {
    pub fn new<'a>(output_path: &'a Path, base_name: &'a str) -> CustomFileWriter<'a> {
        let column_store = ColumnStore::default();
        CustomFileWriter {
            file_name: None,
            base_name: base_name,
            output_path: output_path,
            column_store: column_store,
            write_options: WriteOptions {
                write_statistics: true,
                compression: CompressionOptions::Snappy,
                version: Version::V2,
                data_pagesize_limit: None,
            },
            file_num: 0,
        }
    }

    fn rotate_file(&mut self) {
        let file_name = format!("{}.{}", self.file_num, self.base_name);
        self.file_name = Some(file_name.clone());
        self.file_num += 1;
    }

    pub fn write(&mut self, data: &Vec<TimeItem>, force: bool) -> arrow2::error::Result<()> {
        match self.file_name {
            Some(_) => {}
            None => {
                self.rotate_file();
            }
        }
        if data.len() > 0 {
            self.column_store.append_columns(data);
        }

        if self.column_store.ready_2_write() | force {
            self.write_data()?;

            self.rotate_file();
            self.column_store = ColumnStore::default();
        }
        Ok(())
    }

    fn write_data(&mut self) -> arrow2::error::Result<()> {
        let chunk = self.column_store.build_arrays().unwrap();
        let iter = vec![Ok(chunk)];

        let row_groups = RowGroupIterator::try_new(
            iter.into_iter(),
            &self.column_store.schema,
            self.write_options,
            self.column_store.encodings.clone(),
        )?;

        // Create a new empty file
        let file = fs::File::create(self.output_path.join(&self.file_name.as_ref().unwrap()))?;
        let mut writer =
            FileWriter::try_new(file, self.column_store.schema.clone(), self.write_options)?;

        for group in row_groups {
            writer.write(group?)?;
        }
        let _size = writer.end(None)?;

        Ok(())
    }
}

fn handle_client(
    stream: TcpStream,
    writer: &mut CustomFileWriter,
    re_vehicle: &regex::Regex,
    re_time: &regex::Regex,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(stream);
    let mut timestep = 0.0;
    let mut timestep_vec: Vec<TimeItem> = Vec::new();
    let mut buf = String::new();

    while let Ok(_) = reader.read_line(&mut buf) {
        match re_time.captures(&buf) {
            Some(c) => {
                timestep = c[1].parse::<f64>().unwrap();
            }
            None => match re_vehicle.captures(&buf) {
                Some(v) => {
                    make_vehicle(&mut timestep_vec, timestep, v);
                    if &timestep_vec.len() >= &MAX_ROWS_PER_FILE {
                        let _ = writer.write(&timestep_vec, false).unwrap();
                        timestep_vec.clear();
                    }
                }
                None => match &buf.find("</emission-export>") {
                    Some(_) => break,
                    _ => {}
                },
            },
        }
        buf.clear()
    }

    if timestep_vec.len() > 0 {
        let _ = writer.write(&timestep_vec, false).unwrap();
        timestep_vec.clear();
    }

    Ok(())
}

pub fn socket_emissions(
    socket_string: &str,
    output_path: &str,
    output_base_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let re_vehicle: regex::Regex = regex::Regex::new(r#"id="([^"]*)" eclass="([^"]*)" CO2="([^"]*)" CO="([^"]*)" HC="([^"]*)" NOx="([^"]*)" PMx="([^"]*)" fuel="([^"]*)" electricity="([^"]*)" noise="([^"]*)" route="([^"]*)" type="([^"]*)" waiting="([^"]*)" lane="([^"]*)" pos="([^"]*)" speed="([^"]*)" angle="([^"]*)" x="([^"]*)" y="([^"]*)""#).unwrap();
    let re_time: regex::Regex = regex::Regex::new(r#"<timestep time="([^"]*)""#).unwrap();
    // let re_time_end: regex::Regex = regex::Regex::new(r#"</timestep>"#).unwrap();

    let listener = TcpListener::bind(socket_string)?;

    // make a file writer
    let mut writer = CustomFileWriter::new(Path::new(output_path), output_base_name);

    let buffer_size = 1024 * 1024 * 10;

    // for stream in listener.incoming() {
    let (stream, _) = listener.accept()?;
    handle_client(stream, &mut writer, &re_vehicle, &re_time)?;
    writer.write(&Vec::new(), true)?;
    Ok(())
}

pub fn parse_xml_raw(
    file_path: &str,
    output_path: &str,
    output_base_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_str = std::fs::read_to_string(file_path)?;

    let re_time = regex::Regex::new(r#"<timestep time="(\d+\.\d+)""#).unwrap();
    let re_vehicle = regex::Regex::new(r#"id="([^"]*)" eclass="([^"]*)" CO2="([^"]*)" CO="([^"]*)" HC="([^"]*)" NOx="([^"]*)" PMx="([^"]*)" fuel="([^"]*)" electricity="([^"]*)" noise="([^"]*)" route="([^"]*)" type="([^"]*)" waiting="([^"]*)" lane="([^"]*)" pos="([^"]*)" speed="([^"]*)" angle="([^"]*)" x="([^"]*)" y="([^"]*)""#).unwrap();
    let break_point = regex::Regex::new(r#"</timestep>"#).unwrap();

    let mut writer = CustomFileWriter::new(Path::new(output_path), output_base_name);
    let mut timestep_vec: Vec<TimeItem> = Vec::new();

    re_time.captures_iter(&file_str).for_each(|m| {
        let timestep = m[1].parse::<f64>().unwrap();

        // find the window to search for vehicles
        let start_match = m.get(0).unwrap().end();
        let end_match = break_point.find(&file_str[start_match..]).unwrap().start() + start_match;

        // break if the window is too small
        if start_match >= end_match - 2 {
            return;
        }

        let window = &file_str[start_match..end_match];
        // println!("window: {}", window);
        for v in re_vehicle.captures_iter(window) {
            // println!("v: {:?}", v);
            make_vehicle(&mut timestep_vec, timestep, v);
        }
        if timestep_vec.len() >= MAX_ROWS_PER_FILE {
            let _ = writer.write(&timestep_vec, false).unwrap();
            timestep_vec.clear();
        }
    });

    let _ = writer.write(&timestep_vec, true).unwrap();

    Ok(())
}

fn make_vehicle(timestep_vec: &mut Vec<TimeItem>, timestep: f64, v: regex::Captures<'_>) {
    timestep_vec.push(TimeItem {
        timestep,
        id: v[1].to_owned(),
        eclass: v[2].to_owned(),
        co2: v[3].parse::<f64>().unwrap(),
        co: v[4].parse::<f64>().unwrap(),
        hc: v[5].parse::<f64>().unwrap(),
        nox: v[6].parse::<f64>().unwrap(),
        pmx: v[7].parse::<f64>().unwrap(),
        fuel: v[8].parse::<f64>().unwrap(),
        electricity: v[9].parse::<f64>().unwrap(),
        noise: v[10].parse::<f64>().unwrap(),
        route: v[11].to_owned(),
        type_: v[12].to_owned(),
        waiting: v[13].parse::<f64>().unwrap(),
        lane: v[14].to_owned(),
        pos: v[15].parse::<f64>().unwrap(),
        speed: v[16].parse::<f64>().unwrap(),
        angle: v[17].parse::<f64>().unwrap(),
        x: v[18].parse::<f64>().unwrap(),
        y: v[19].parse::<f64>().unwrap(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_emissions_xml_to_df2() {
        parse_xml_raw(
            "tests/test_data/emissions.xml",
            "tests/test_data/",
            "emissions.parquet",
        )
        .unwrap();
    }

    #[test]
    fn test_socket_listener() {
        let _ = socket_emissions("127.0.0.1:5555", "tests/test_data", "emissions_socket");
    }
}
