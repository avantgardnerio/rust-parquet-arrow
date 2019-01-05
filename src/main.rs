#![feature(try_from)]

extern crate parquet;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use std::convert::TryFrom;
use std::env;
use std::fs::File;
use std::path::Path;
use std::borrow::Borrow;
use parquet::schema::types::Type::PrimitiveType;
use parquet::schema::types::Type::GroupType;

fn main() {
    let args: Vec<_> = env::args().collect();
    let path = &args[1];
    println!("Reading path: {}", path);

    // schema
    let reader = SerializedFileReader::try_from(path.to_string()).unwrap();
    let parquet_metadata = reader.metadata();
    let file_metadata = parquet_metadata.file_metadata();
    let schema = file_metadata.schema();
    let slice = schema.get_fields();
    for element in slice.iter() {
        match element.borrow() {
            PrimitiveType{basic_info, physical_type, type_length, scale,precision} => {
                println!("primitive {:?} {:?}", basic_info.name(), physical_type);
            },
            GroupType{basic_info, fields} => {
                println!("group {:?} {:?}", basic_info.name(), basic_info.logical_type());
            }
        }
    }

    // data
    let file = File::open(&Path::new(&path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    while let Some(record) = iter.next() {
        record.get_bool(0);
        println!("{}", record);
    }
}
