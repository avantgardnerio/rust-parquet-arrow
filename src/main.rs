#![feature(try_from)]

extern crate arrow;
extern crate parquet;

use arrow::datatypes::*;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use parquet::basic::LogicalType;
use parquet::schema::types::Type::GroupType;
use parquet::schema::types::Type::PrimitiveType;
use std::any::Any;
use std::env;
use std::fs::File;
use std::path::Path;

fn main() {
    let args: Vec<_> = env::args().collect();
    let path = &args[1];
    println!("Reading path: {}", path);

    // schema
    let file = File::open(&Path::new(&path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();
    let file_metadata = parquet_metadata.file_metadata();
    let schema = file_metadata.schema();
    let fields: Vec<Field> = schema.get_fields().iter().map(|x| type2field(x)).collect();
    let mut data: Vec<Box<Any>> = schema.get_fields().iter().map(|x| type2vec(x)).collect();

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

fn type2vec(t: &parquet::schema::types::Type) -> Box<Any> {
    use parquet::basic::Type;
    match t {
        PrimitiveType {
            basic_info,
            physical_type,
            type_length: _,
            scale: _,
            precision: _
        } => {
            match physical_type {
                Type::BOOLEAN => {
                    return Box::new(Vec::<bool>::new());
                }
                Type::INT32 => {
                    return Box::new(Vec::<i32>::new());
                },
                Type::INT64 => {
                    return Box::new(Vec::<i64>::new());
                },
                Type::INT96 => {
                    panic!("INT96 not implemented!");
                },
                Type::FLOAT => {
                    return Box::new(Vec::<f32>::new());
                },
                Type::DOUBLE => {
                    return Box::new(Vec::<f64>::new());
                },
                Type::BYTE_ARRAY => {
                    match basic_info.logical_type() {
                        LogicalType::UTF8 => {
                            return Box::new(Vec::<String>::new());
                        }
                        _ => {
                            panic!("Unknown type!");
                        }
                    }
                },
                Type::FIXED_LEN_BYTE_ARRAY => {
                    panic!("Unknown type!");
                }
            }
        },
        GroupType { basic_info: _, fields: _ } => {
            panic!("Unknown type!");
        }
    }
}

fn type2field(t: &parquet::schema::types::Type) -> Field {
    use parquet::basic::Type;
    match t {
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
            let name = basic_info.name();
            match physical_type {
                Type::BOOLEAN => {
                    return Field::new(name, DataType::Boolean, nullable);
                },
                Type::INT32 => {
                    return Field::new(name, DataType::Int32, nullable);
                },
                Type::INT64 => {
                    return Field::new(name, DataType::Int64, nullable);
                },
                Type::INT96 => {
                    panic!("INT96 not implemented!");
                },
                Type::FLOAT => {
                    return Field::new(name, DataType::Float32, nullable);
                },
                Type::DOUBLE => {
                    return Field::new(name, DataType::Float64, nullable);
                },
                Type::BYTE_ARRAY => {
                    match basic_info.logical_type() {
                        LogicalType::UTF8 => {
                            return Field::new(name, DataType::Utf8, nullable);
                        }
                        _ => {
                            panic!("Unknown type!");
                        }
                    }
                },
                parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => {
                    panic!("Unknown type!");
                }
            }
        },
        GroupType { basic_info: _, fields: _ } => {
            panic!("Unknown type!");
        }
    };
}