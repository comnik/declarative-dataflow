//! Converts benchmark data into datoms that can be directly read into
//! a collection.

extern crate declarative_dataflow;
extern crate parquet;
extern crate timely;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::rc::Rc;

use parquet::column::writer::{get_typed_column_writer, ColumnWriter, ColumnWriterImpl};
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::parser::parse_message_type;

fn main() {
    let in_path = Path::new("../data/labelprop/httpd_df.dms");
    let in_file = BufReader::new(File::open(&in_path).unwrap());

    let out_path = Path::new("../data/labelprop/httpd_df.parquet");
    let out_file = File::create(out_path).unwrap();

    let message_type = "message schema { REQUIRED INT64 src; }";
    let schema = Rc::new(parse_message_type(message_type).unwrap());
    let props = Rc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(out_file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    let col_writer = row_group_writer.next_column().unwrap().unwrap();
    let mut node_writer: ColumnWriterImpl<Int64Type> = get_typed_column_writer(col_writer);
    // let mut edge_writer: ColumnWriterImpl<Int64Type> = get_typed_column_writer(row_group_writer.next_column().unwrap().unwrap());

    // let mut line_count = 0;

    for readline in in_file.lines() {
        // if line_count >= 100 { break; }
        // line_count += 1;

        let line = readline.ok().expect("read error");

        if !line.starts_with('#') && line.len() > 0 {
            let mut elts = line[..].split_whitespace();
            let src: i64 = elts.next().unwrap().parse().ok().expect("malformed src");
            let dst: i64 = elts.next().unwrap().parse().ok().expect("malformed dst");
            let typ: &str = elts.next().unwrap();

            match typ {
                "n" => {
                    node_writer.write_batch(&[src], None, None).unwrap();
                }
                "e" => { /*edge_writer.write_batch(&[dst], None, None);*/ }
                unk => panic!("unknown type: {}", unk),
            }
        }
    }

    println!("wrote all lines");

    row_group_writer
        .close_column(ColumnWriter::Int64ColumnWriter(node_writer))
        .unwrap();

    writer.close_row_group(row_group_writer).unwrap();
    writer.close().unwrap();
}
