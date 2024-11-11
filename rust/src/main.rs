use std::collections::HashMap;
use std::{fmt, thread};
use std::cmp::{max, min};
use std::fmt::{Display, Formatter};
use std::io::prelude::*;
use std::fs::File;
use std::time::Instant;
use memmap2::Mmap;

#[derive(Clone)]
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

    pub fn add(&self, other: Collector) -> Collector {
        Collector {
            min: min(self.min, other.min),
            max: max(self.max, other.max),
            count: self.count + other.count,
            sum: self.sum + other.sum
        }
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
            f, "{} {} {}",
            (self.sum as f64 / 10.0) / self.count as f64,
            self.min as f64 / 10.0,
            self.max as f64 / 10.0
        )
    }
}

fn process_line(ln: &str, cities: &mut HashMap<String, Collector>) {
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

fn get_next_n_chars(mmap: &Mmap, start: usize, n_chars: usize) -> (usize, usize) {
    let mut end = start + n_chars;
    if end >= mmap.len() {
        return (start, mmap.len())
    }

    let char_at_end = mmap.get(end);
    // Take from start .. end of memory
    if char_at_end.is_none() {
        return (start, mmap.len());
    }
    if *char_at_end.unwrap() == b'\n' {
        return (start, end)
    }

    let mut current = char_at_end.unwrap();
    // If not at newline keep iterating until we fine one
    while *current != b'\n' {
        end += 1;
        // Can unwrap as has to end with newline
        current = mmap.get(end).unwrap()
    }
    (start, end)
}

fn main() {
    let start_time = Instant::now();
    let fp = "/home/lee/Projects/1brc-data/1brc/measurements.txt";
    let file = File::open(fp).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };

    let n_chars = 500_000_000;
    let mut handles = vec![];
    let (mut start_char_index, mut end_char_index) = get_next_n_chars(&mmap, 0, n_chars);

    let mut count = 0;
    while end_char_index < mmap.len() {
        let data_chunk = String::from_utf8_lossy(&mmap[start_char_index..end_char_index]).to_string();

        // count += 1;
        // if count > 5 {
        //     break
        // }

        let handle = thread::spawn(move || {
            println!("Reading chars between {} and {}", start_char_index, end_char_index);
            let mut cities: HashMap<String, Collector> = HashMap::new();

            data_chunk.split("\n").for_each(
                |l| {
                    if l.len() > 1 {
                        process_line(l, &mut cities);
                    }
                }
            );
            println!("Finished chars lines to {}", end_char_index);
            cities
        });
        handles.push(handle);

        (start_char_index, end_char_index) = get_next_n_chars(&mmap, end_char_index, n_chars);
    }


    let mut final_cities: HashMap<String, Collector> = HashMap::new();
    for handle in handles {
        let cities = handle.join().unwrap();
        cities.iter().for_each(|(city, collector)| {
            if final_cities.contains_key(city) {
                let existing = final_cities.get(city).unwrap();
                final_cities.insert(String::from(city), existing.add(collector.clone()));
            } else {
                final_cities.insert(String::from(city), collector.clone());
            }
        })
    }

    let duration = start_time.elapsed();

    final_cities.iter().for_each(|(city, collector)| {
        println!("{} {}", city, collector);
    });

    println!("Elapsed time: {} ms", duration.as_millis());
}

// TODO
// threading
// create collector struct in thread
// combine collectors at end

// mmap
// better buffered reading?

// save as json

