#![feature(try_from)]

extern crate parquet;
extern crate arrow;

use arrow::datatypes::*;
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
    let mut fields:Vec<Field> = Vec::new();
    let reader = SerializedFileReader::try_from(path.to_string()).unwrap();
    let parquet_metadata = reader.metadata();
    let file_metadata = parquet_metadata.file_metadata();
    let schema = file_metadata.schema();
    let slice = schema.get_fields();
    for element in slice.iter() {
        match element.borrow() {
            PrimitiveType{basic_info, physical_type, type_length, scale,precision} => {
                match physical_type {
                    parquet::basic::Type::DOUBLE => {
                        let nullable = if basic_info.has_repetition() { basic_info.repetition() != parquet::basic::Repetition::REQUIRED} else {false};
                        let field = Field::new(
                            basic_info.name(),
                            DataType::Float64,
                            nullable
                        );
                        fields.push(field);
                        println!("DOUBLE {:?}", basic_info.name())
                    },
                    parquet::basic::Type::BOOLEAN => println!("BOOLEAN {:?}", basic_info.name()),
                    parquet::basic::Type::INT32 => println!("INT32 {:?}", basic_info.name()),
                    parquet::basic::Type::INT64 => println!("INT64 {:?}", basic_info.name()),
                    parquet::basic::Type::INT96 => println!("INT96 {:?}", basic_info.name()),
                    parquet::basic::Type::FLOAT => println!("FLOAT {:?}", basic_info.name()),
                    parquet::basic::Type::BYTE_ARRAY => println!("BYTE_ARRAY {:?}", basic_info.name()),
                    parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => println!("FIXED_LEN_BYTE_ARRAY {:?}", basic_info.name())
                }
            },
            GroupType{basic_info, fields} => {
//                match basic_info.logical_type() {
//                    parquet::basic::LogicalType::LIST => {
//                    }
//                }
                println!("group {:?}", basic_info);
                for field in fields.iter() {
                    println!("   field {:?} {:?}", field.name(), field.get_basic_info());
                }
            }
        }
    }

    // create arrow schema
    let schema = Schema::new(fields);

    // data
    let file = File::open(&Path::new(&path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    while let Some(record) = iter.next() {
        record.get_bool(0);
        println!("{}", record);
    }
}
