use std::collections::HashMap;
use std::{fmt, thread};
use std::fmt::{Display, Formatter};
use std::io::prelude::*;
use std::fs::File;
use std::time::Instant;
use memmap2::Mmap;

struct Collector {
    min: i32,
    max: i32,
    count: i32,
    sum: i32
}

impl Collector {
    pub fn new(starting_val: i32) -> Collector {
        Collector {min: starting_val, max: starting_val, count: 1, sum: starting_val}
    }

    pub fn set_min(&mut self, new_val: i32) {
        if new_val < self.min {
            self.min = new_val;
        }
    }

    pub fn set_max(&mut self, new_val: i32) {
        if new_val > self.max {
            self.max = new_val;
        }
    }

    pub fn update_count(&mut self) {
        self.count += 1
    }

    pub fn update_sum(&mut self, new_val: i32) {
        self.sum += new_val;
    }

    pub fn update_for_val(&mut self, new_val: i32) {
        self.set_min(new_val);
        self.set_max(new_val);
        self.update_sum(new_val);
        self.update_count();
    }
}

impl Display for Collector {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f, "{} {} {} {}",
            self.count,
            self.sum,
            self.min,
            self.max
        )
    }
}

fn process_line(ln: &str) {
    let mut cities: HashMap<String, Collector> = HashMap::new();

    let vals: Vec<String> = ln.split(';')
        .map(|s| String::from(s)).collect();
    let (city, reading) = (&vals[0], &vals[1]);

    // Convert float string to integer for faster arithmetic
    // E.g. 42.1 becomes 421
    // TODO check this is actually helpful
    let temp = reading.parse::<f32>().expect("Error parsing temperature");
    let temp_normalised = (temp * 10.0) as i32;

    match cities.contains_key(city.as_str()) {
        false => {
            cities.insert(
                city.to_string(),
                Collector::new(temp_normalised)
            );
        }
        true => {
            cities.get_mut(city.as_str()).unwrap()
                .update_for_val(temp_normalised);
        }
    };
}

fn get_lines_range(mmap: &Mmap, start_line: usize, num_lines: usize) -> Option<(usize, usize)> {
    let mut line_count = 0;
    let mut start_index = 0;
    let mut end_index = 0;

    for (i, &byte) in mmap.iter().enumerate() {
        if byte == b'\n' {
            line_count += 1;

            if line_count == start_line {
                start_index = i + 1; // Start of the first line
            }

            // If we've reached the n-th line
            if line_count == start_line + num_lines {
                end_index = i; // End of the nth line
                break;
            }
        }
    }

    // TODO fix this to return exact number
    // Probably just return end index?
    if line_count >= start_line + num_lines {
        Some((start_index, end_index))
    } else {
        None // If there are not enough lines
    }
}

fn main() {
    let start_time = Instant::now();

    let fp = "/home/lee/Projects/1brc-data/1brc/measurements.txt";
    let file = File::open(fp).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };

    let mut start_line = 0;
    let num_lines = 100_000_000;

    let mut handles = vec![];
    while let Some((start_index, end_index)) = get_lines_range(&mmap, start_line, num_lines) {
        let data_chunk = String::from_utf8_lossy(&mmap[start_index..end_index]).to_string();

        let handle = thread::spawn(move || {
            println!("Readling lines from {}", start_line);
            data_chunk.split("\n").for_each(
                |l| process_line(l)
            );
            println!("Finished reading lines from {}", start_line);
        });
        handles.push(handle);

        start_line += num_lines;
    }
    for handle in handles {
        handle.join().unwrap();
    }

    // for (key, value) in &cities {
    //     println!("City: {}, res: {}", key, value);
    // }

    let duration = start_time.elapsed();
    println!("Elapsed time: {} ms", duration.as_millis());
}

// TODO
// threading
// create collector struct in thread
// combine collectors at end

// mmap
// better buffered reading?

// save as json

