use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io::prelude::*;
use std::io::BufReader;
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

fn process_line(ln: &str, cities: &mut HashMap<String, Collector>) {
    let vals: Vec<String> = ln.split(';')
        .map(|s| String::from(s)).collect();
    let (city, reading) = (&vals[0], &vals[1]);

    // Convert float string to integer for faster arithmetic
    // E.g. 42.1 becomes 421
    let temp = reading.parse::<f32>().unwrap();
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

fn main() {
    let start = Instant::now();

    let mut cities: HashMap<String, Collector> = HashMap::new();

    let fp = "/home/lee/Projects/1brc-data/1brc/measurements.txt";
    let file = File::open(fp).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };
    let content = std::str::from_utf8(&mmap).unwrap();
    let lines: Vec<&str> = content.lines().collect();

    let mut count = 0;
    for line in lines {
        count += 1;
        process_line(line, &mut cities);

        if count % 1000000 == 0 {
            println!("On line {}", count);
        }

        // if count > 10_000_000 {
        //     break
        // }
    }
    println!("DONEZO");
    for (key, value) in &cities {
        println!("City: {}, res: {}", key, value);
    }

    let duration = start.elapsed();
    println!("Elapsed time: {} ms", duration.as_millis());
}

// TODO
// threading
// mmap
// better buffered reading?

