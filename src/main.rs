#![feature(try_from)]

extern crate arrow;
extern crate parquet;

use arrow::datatypes::*;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use parquet::schema::types::Type::GroupType;
use parquet::schema::types::Type::PrimitiveType;
use std::any::Any;
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::env;
use std::fs::File;
use std::path::Path;

fn main() {
    let args: Vec<_> = env::args().collect();
    let path = &args[1];
    println!("Reading path: {}", path);

    // schema
    let mut fields: Vec<Field> = Vec::new();
    let mut data: Vec<Box<Any>> = Vec::new();
    let reader = SerializedFileReader::try_from(path.to_string()).unwrap();
    let parquet_metadata = reader.metadata();
    let file_metadata = parquet_metadata.file_metadata();
    let schema = file_metadata.schema();
    let slice = schema.get_fields();
    for element in slice.iter() {
        match element.borrow() {
            PrimitiveType { basic_info, physical_type, type_length, scale, precision } => {
                let nullable = if basic_info.has_repetition() {
                    basic_info.repetition() != parquet::basic::Repetition::REQUIRED
                } else {
                    false
                };
                match physical_type {
                    parquet::basic::Type::DOUBLE => {
                        let field = Field::new(basic_info.name(), DataType::Float64, nullable);
                        fields.push(field);
                        let mut col: Vec<f64> = Vec::new();
                        data.push(Box::new(col));
                        println!("DOUBLE {:?}", basic_info.name());
                    }
                    parquet::basic::Type::BOOLEAN => {
                        let field = Field::new(basic_info.name(), DataType::Boolean, nullable);
                        fields.push(field);
                        let mut col: Vec<bool> = Vec::new();
                        data.push(Box::new(col));
                        println!("BOOLEAN {:?}", basic_info.name());
                    }
                    parquet::basic::Type::INT32 => println!("INT32 {:?}", basic_info.name()),
                    parquet::basic::Type::INT64 => println!("INT64 {:?}", basic_info.name()),
                    parquet::basic::Type::INT96 => println!("INT96 {:?}", basic_info.name()),
                    parquet::basic::Type::FLOAT => println!("FLOAT {:?}", basic_info.name()),
                    parquet::basic::Type::BYTE_ARRAY => println!("BYTE_ARRAY {:?}", basic_info.name()),
                    parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => println!("FIXED_LEN_BYTE_ARRAY {:?}", basic_info.name())
                }
            }
            GroupType { basic_info, fields } => {
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

    // data
    let file = File::open(&Path::new(&path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    while let Some(record) = iter.next() {
        for i in 0..record.len() {
            match fields[i].data_type() {
                DataType::Float64 => {
                    let val = record.get_double(i).unwrap();
                    let mut boxed = data.get_mut(i).unwrap();
                    let mut column: &Vec<f64> = boxed.downcast_mut::<Vec<f64>>().unwrap();
                    column.push(val);
                },
                _ => panic!("Unknown type!")
            }
        }
        println!("{}", record.len());
    }

    // create arrow schema
    let schema = Schema::new(fields);

//    let batch = arrow::record_batch::RecordBatch::new(
//        std::sync::Arc::new(schema),
//        data
//    );
}
