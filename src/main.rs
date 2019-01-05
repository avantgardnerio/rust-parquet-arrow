extern crate parquet;

use std::env;
use std::fs::File;
use std::path::Path;
use parquet::record::RowAccessor;
use parquet::file::reader::{FileReader, SerializedFileReader};

fn main() {
    let args: Vec<_> = env::args().collect();
    let path = &args[1];
    println!("Reading path: {}", path);
    let file = File::open(&Path::new(&path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    while let Some(record) = iter.next() {
        record.get_bool(0);
        println!("{}", record);
    }
}
