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
    let file = File::open(&Path::new(&path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();
    let file_metadata = parquet_metadata.file_metadata();
    let schema = file_metadata.schema();
    let slice = schema.get_fields();
    let mut i = 0;
    for element in slice.iter() {
        println!("el[{}]: {:?}", i, element);
        i += 1;
        match element.borrow() {
            PrimitiveType {
                basic_info,
                physical_type,
                type_length: _,
                scale: _,
                precision: _
            } => {
                let nullable = if basic_info.has_repetition() {
                    basic_info.repetition() != parquet::basic::Repetition::REQUIRED
                } else {
                    false
                };
                match physical_type {
                    parquet::basic::Type::BOOLEAN => {
                        fields.push(Field::new(basic_info.name(), DataType::Boolean, nullable));
                        data.push(Box::new(Vec::<bool>::new()));
                    }
                    parquet::basic::Type::INT32 => {
                        fields.push(Field::new(basic_info.name(), DataType::Int32, nullable));
                        data.push(Box::new(Vec::<i32>::new()));
                    },
                    parquet::basic::Type::INT64 => {
                        fields.push(Field::new(basic_info.name(), DataType::Int64, nullable));
                        data.push(Box::new(Vec::<i64>::new()));
                    },
                    parquet::basic::Type::INT96 => panic!("INT96 not implemented!"),
                    parquet::basic::Type::FLOAT => {
                        fields.push(Field::new(basic_info.name(), DataType::Float32, nullable));
                        data.push(Box::new(Vec::<f32>::new()));
                    },
                    parquet::basic::Type::DOUBLE => {
                        fields.push(Field::new(basic_info.name(), DataType::Float64, nullable));
                        data.push(Box::new(Vec::<f64>::new()));
                    }
                    parquet::basic::Type::BYTE_ARRAY => {
                        match basic_info.logical_type() {
                            parquet::basic::LogicalType::UTF8 => {
                                fields.push(Field::new(basic_info.name(), DataType::Utf8, nullable));
                                let mut col: Vec<String> = Vec::new();
                                data.push(Box::new(col));
                            }
                            _ => {}
                        }
                    },
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
            println!("Processing field {}", i);
            match fields[i].data_type() {
                DataType::Float64 => {
                    let val = record.get_double(i).unwrap();
                    let column: &mut Vec<f64> = data[i].downcast_mut().unwrap();
                    column.push(val);
                },
                DataType::Utf8 => {
                    let val = record.get_string(i).unwrap().clone();
                    let column: &mut Vec<String> = data[i].downcast_mut().unwrap();
                    column.push(val);
                },
                _ => panic!("Unknown type: {:?}", fields[i].data_type())
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
