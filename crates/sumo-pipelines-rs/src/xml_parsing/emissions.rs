use std::fs::File;
use std::io::BufReader;
use polars::prelude::*;
use quick_xml::de::from_reader;
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Default)]
#[serde(default)]
// #[serde(rename = "timestep")]
struct Timestep {
    #[serde(rename = "@time")]
    time: String,
    vehicle: Vec<Vehicle>,
}

#[derive(Debug, Deserialize, PartialEq, Default)]
#[serde(default)]
// #[serde(rename = "vehicle")]
struct Vehicle {
    #[serde(rename = "@id")]
    id: String,
    #[serde(rename = "@eclass")]
    eclass: String,
    #[serde(rename = "@CO2")]
    co2: String,
    #[serde(rename = "@CO")]
    co: String,
    #[serde(rename = "@HC")]
    hc: String,
    #[serde(rename = "@NOx")]
    nox: String,
    #[serde(rename = "@PMx")]
    pmx: String,
    #[serde(rename = "@fuel")]
    fuel: String,
    #[serde(rename = "@electricity")]
    electricity: String,
    #[serde(rename = "@noise")]
    noise: String,
    #[serde(rename = "@route")]
    route: String,
    #[serde(rename = "@type")]
    type_: String,
    #[serde(rename = "@waiting")]
    waiting: String,
    #[serde(rename = "@lane")]
    lane: String,
    #[serde(rename = "@pos")]
    pos: String,
    #[serde(rename = "@speed")]
    speed: String,
    #[serde(rename = "@angle")]
    angle: String,
    #[serde(rename = "@x")]
    x: String,
    #[serde(rename = "@y")]
    y: String,
}


#[derive(Debug, PartialEq, Default, Deserialize)]
#[serde(rename = "emission-export")]
struct EmissionExport {
    timestep: Vec<Timestep>,
}

impl EmissionExport {
    pub fn build_dataframe(&self) -> Result<DataFrame, Box<dyn std::error::Error>>{
        let mut timestep: Vec<AnyValue> = Vec::new();
        let mut id: Vec<AnyValue> = Vec::new();
        let mut eclass: Vec<AnyValue> = Vec::new();
        let mut co2: Vec<AnyValue> = Vec::new();
        let mut co: Vec<AnyValue> = Vec::new();
        let mut hc: Vec<AnyValue> = Vec::new();
        let mut nox: Vec<AnyValue> = Vec::new();
        let mut pmx: Vec<AnyValue> = Vec::new();
        let mut fuel: Vec<AnyValue> = Vec::new();
        let mut electricity: Vec<AnyValue> = Vec::new();
        let mut noise: Vec<AnyValue> = Vec::new();
        let mut route: Vec<AnyValue> = Vec::new();
        let mut type_: Vec<AnyValue> = Vec::new();
        let mut waiting: Vec<AnyValue> = Vec::new();
        let mut lane: Vec<AnyValue> = Vec::new();
        let mut pos: Vec<AnyValue> = Vec::new();
        let mut speed: Vec<AnyValue> = Vec::new();
        let mut angle: Vec<AnyValue> = Vec::new();
        let mut x: Vec<AnyValue> = Vec::new();
        let mut y: Vec<AnyValue> = Vec::new();

        for t in &self.timestep {
            for v in &t.vehicle {
                timestep.push(AnyValue::Float64(t.time.parse::<f64>().unwrap()));
                id.push(AnyValue::Utf8(&v.id));
                eclass.push(AnyValue::Utf8(&v.eclass));
                co2.push(AnyValue::Float64(v.co2.parse::<f64>().unwrap()));
                co.push(AnyValue::Float64(v.co.parse::<f64>().unwrap()));
                hc.push(AnyValue::Float64(v.hc.parse::<f64>().unwrap()));
                nox.push(AnyValue::Float64(v.nox.parse::<f64>().unwrap()));
                fuel.push(AnyValue::Float64(v.fuel.parse::<f64>().unwrap()));
                pmx.push(AnyValue::Float64(v.pmx.parse::<f64>().unwrap()));
                electricity.push(AnyValue::Float64(v.electricity.parse::<f64>().unwrap()));
                noise.push(AnyValue::Float64(v.noise.parse::<f64>().unwrap()));
                route.push(AnyValue::Utf8(&v.route));
                type_.push(AnyValue::Utf8(&v.type_));
                waiting.push(AnyValue::Float64(v.waiting.parse::<f64>().unwrap()));
                lane.push(AnyValue::Utf8(&v.lane));
                pos.push(AnyValue::Float64(v.pos.parse::<f64>().unwrap()));
                speed.push(AnyValue::Float64(v.speed.parse::<f64>().unwrap()));
                angle.push(AnyValue::Float64(v.angle.parse::<f64>().unwrap()));
                x.push(AnyValue::Float64(v.x.parse::<f64>().unwrap()));
                y.push(AnyValue::Float64(v.y.parse::<f64>().unwrap()));
            }
        }

        let df = DataFrame::new(vec![
            Series::new("timestep", timestep),
            Series::new("id", id),
            Series::new("eclass", eclass),
            Series::new("co2", co2),
            Series::new("co", co),
            Series::new("hc", hc),
            Series::new("nox", nox),
            Series::new("pmx", pmx),
            Series::new("fuel", fuel),
            Series::new("electricity", electricity),
            Series::new("noise", noise),
            Series::new("route", route),
            Series::new("type", type_),
            Series::new("waiting", waiting),
            Series::new("lane", lane),
            Series::new("pos", pos),
            Series::new("speed", speed),
            Series::new("angle", angle),
            Series::new("x", x),
            Series::new("y", y),
        ])?;
        Ok(df)

    }
}

pub fn parse_xml_to_df(file_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file: File = File::open(file_path)?;
    // read the file into a string
    let file = BufReader::new(file);
    // let file = BufReader::new(file);
    let res: EmissionExport = from_reader(file)?;
    let mut df = res.build_dataframe()?;

    ParquetWriter::new(std::fs::File::create(output_path).unwrap())
        .with_statistics(true)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)
        .unwrap();

    Ok(())
}
