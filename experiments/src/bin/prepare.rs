//! Converts benchmark data into datoms that can be directly read into
//! a collection.

extern crate timely;
extern crate declarative_dataflow;

use std::fs::{File};
use std::io::{BufRead, BufReader, Write, BufWriter};
use std::path::{Path};

fn main() {

    let in_path = Path::new("../data/labelprop/pgs_df.dms");
    let in_file = BufReader::new(File::open(&in_path).unwrap());

    let edges = File::create(&Path::new("../data/labelprop/edges.pgs_df")).unwrap();
    let nodes = File::create(&Path::new("../data/labelprop/nodes.pgs_df")).unwrap();
    
    let mut edge_writer = BufWriter::new(edges);
    let mut node_writer = BufWriter::new(nodes);

    for readline in in_file.lines() {

        let line = readline.expect("read error");

        if !line.starts_with('#') && !line.is_empty() {

            let mut elts = line[..].split_whitespace();
            let e = elts.next().unwrap();
            let v = elts.next().unwrap();
            let attr: &str = elts.next().unwrap();

            let writer = match attr {
                "e" => &mut edge_writer,
                "n" => &mut node_writer,
                unk => panic!("unknown type: {}", unk),
            };

            writer.write_all(e.as_bytes()).unwrap();
            writer.write_all(b" ").unwrap();
            writer.write_all(v.as_bytes()).unwrap();
            writer.write_all(b"\n").unwrap();
        }
    }

    edge_writer.flush().unwrap();
    node_writer.flush().unwrap();
}
