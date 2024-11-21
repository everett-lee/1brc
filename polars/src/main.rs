
use polars::prelude::*;
use polars::lazy::dsl::sum;
use std::fs::File;
use std::time::Instant;
use polars::export::num::real::Real;

fn main() {
    let query = LazyCsvReader::new("../measurements.txt")
        .with_separator(b';')
        .with_has_header(false)
        .finish().unwrap();

    let start_time = Instant::now();

    let mut res =query
        .with_streaming(true)
        .with_columns([
            col("column_1").alias("city"),
            col("column_2").alias("reading"),
        ])
        .group_by([col("city")])
        .agg(
            [
                min("reading").alias("min_reading"),
                avg("reading").alias("avg_reading"),
                max("reading").alias("max_reading")
            ]
        )
        .sort(["city"], Default::default())
        .collect()
        .unwrap();

    let mut file = File::create("averages-polars.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut res).unwrap();
    let duration = start_time.elapsed();
    println!("Elapsed time: {} ms", duration.as_millis());
}
