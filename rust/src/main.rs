mod helpers;

use std::collections::HashMap;
use std::{fmt, fs, thread};
use std::fmt::{Display, Formatter};
use std::io::prelude::*;
use std::fs::File;
use std::time::Instant;
use memmap2::Mmap;

#[derive(Clone)]
struct Collector {
    min: f32,
    max: f32,
    count: i32,
    sum: f32
}

impl Collector {
    pub fn new(starting_val: f32) -> Collector {
        Collector {min: starting_val, max: starting_val, count: 1, sum: starting_val}
    }

    pub fn add(&self, other: Collector) -> Collector {
        Collector {
            min: f32::min(self.min, other.min),
            max: f32::max(self.max, other.max),
            count: self.count + other.count,
            sum: self.sum + other.sum
        }
    }

    pub fn set_min(&mut self, new_val: f32) {
        if new_val < self.min {
            self.min = new_val;
        }
    }

    pub fn set_max(&mut self, new_val: f32) {
        if new_val > self.max {
            self.max = new_val;
        }
    }

    pub fn update_count(&mut self) {
        self.count += 1
    }

    pub fn update_sum(&mut self, new_val: f32) {
        self.sum += new_val;
    }

    pub fn update_for_val(&mut self, new_val: f32) {
        self.set_min(new_val);
        self.set_max(new_val);
        self.update_sum(new_val);
        self.update_count();
    }
}

impl Display for Collector {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f, "{:.1}/{:.1}/{:.1}",
            self.min,
            ((self.sum / self.count as f32) * 10.0).round() / 10.0,
            self.max
        )
    }
}

fn process_line(ln: &str, cities: &mut HashMap<String, Collector>) {
    let vals: Vec<String> = ln.split(';')
        .map(|s| String::from(s)).collect();

    let (city, reading) = (&vals[0], &vals[1]);

    let temp = reading.parse::<f32>().expect("Error parsing temperature");
    match cities.contains_key(city.as_str()) {
        false => {
            cities.insert(
                city.to_string(),
                Collector::new(temp)
            );
        }
        true => {
            cities.get_mut(city.as_str()).unwrap()
                .update_for_val(temp);
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

fn read_expected_as_hashmap() -> HashMap<String, String> {
    let content = fs::read_to_string("averages.txt").unwrap();

    let trimmed = content.trim().trim_start_matches('{').trim_end_matches('}');
    let pairs: Vec<&str> = trimmed.split(',').collect();

    let mut city_to_stats = HashMap::new();
    for pair in pairs {
        let mut kv = pair.splitn(2, '=');
        if let (Some(key), Some(value)) = (kv.next(), kv.next()) {
            city_to_stats.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    city_to_stats
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



    let expected = read_expected_as_hashmap();
    final_cities.iter().for_each(|(city, col)| {
        println!("Comparing for city {}", &city);
       let matching = expected.get(city).expect(&format!("Map should contain city {city}"));
        assert_eq!(matching, &col.to_string())
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

