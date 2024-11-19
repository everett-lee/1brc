mod helpers;
mod collector;

use std::collections::HashMap;
use std::{thread};
use std::fmt::{Display};
use std::io::prelude::*;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
use memmap2::Mmap;
use crate::collector::Collector;

/// Split a line on the ';' separator and parse each side, yielding
/// city_name, reading
/// Use these update the Collector for this city
fn process_line(ln: &[u8], cities: &mut HashMap<String, Collector>) {
    let vals: Vec<String> = ln.split(|&byte| byte == b';')
        .map(|s| String::from(std::str::from_utf8(s).expect("Invalid UTF-8")))
        .collect();

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

/// Skip forward in mapped measurements.txt file n_chars at a time from start
/// If the end char is not a newline, continue checking next character until it is
/// After this process (start, end) will reference a section of the file containing
/// contiguous lines e.g.
/// Beijing;14.7
/// Yangon;23.7
/// Amsterdam;9.1
/// Nakhon Ratchasima;12.0
fn get_next_n_chars(mmap: &Mmap, start: usize, n_chars: usize) -> (usize, usize) {
    let mut end = start + n_chars;
    if end >= mmap.len() {
        return (start, mmap.len())
    }

    let char_at_end = mmap.get(end);
    // Past range of mapped file, take from start .. end of memory
    if char_at_end.is_none() {
        return (start, mmap.len());
    }
    // Reached end of line
    if *char_at_end.unwrap() == b'\n' {
        return (start, end)
    }

    let mut current = char_at_end.unwrap();
    // If not at end of line keep iterating until we are
    while *current != b'\n' {
        end += 1;
        // Can unwrap as has to end with newline
        current = mmap.get(end).unwrap()
    }
    (start, end)
}


fn main() {
    let start_time = Instant::now();
    let fp = "../measurements.txt";
    let file = File::open(fp).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };
    let mmap_arc = Arc::new(mmap);

    let n_chars = 250_000_000;
    let mut handles = vec![];

    let mmap_outer = Arc::clone(&mmap_arc);
    let (mut start_char_index, mut end_char_index) = get_next_n_chars(&mmap_outer, 0, n_chars);
    while end_char_index <= mmap_outer.len() {
        let mmap_inner = Arc::clone(&mmap_arc);

        let handle = thread::spawn(move || {
            let data_chunk = &mmap_inner[start_char_index..end_char_index];

            println!("Reading chars between {} and {}", start_char_index, end_char_index);
            let mut cities: HashMap<String, Collector> = HashMap::with_capacity(120);

            data_chunk.split(|&byte| byte == b'\n').for_each(
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

        // Reached last char of file
        if end_char_index == mmap_outer.len() {
            break
        }
        (start_char_index, end_char_index) = get_next_n_chars(&mmap_outer, end_char_index, n_chars);
    }

    let mut final_cities: HashMap<String, Collector> = HashMap::with_capacity(120);
    for handle in handles {
        let thread_cities = handle.join().unwrap();
        thread_cities.iter().for_each(|(city, collector)| {
            match final_cities.contains_key(city) {
                true => {
                    let existing = final_cities.get(city).unwrap();
                    final_cities.insert(String::from(city), existing.add(collector.clone()));
                }
                false => {
                    final_cities.insert(String::from(city), collector.clone());
                }
            }
        })
    }

    let duration = start_time.elapsed();

    let expected = helpers::read_expected_as_hashmap();
    final_cities.iter().for_each(|(city, col)| {
        println!("Comparing for city {}", &city);
        let matching = expected.get(city).expect(&format!("Map should contain city {city}"));
        assert_eq!(matching, &col.to_string())
    });
    println!("Elapsed time: {} ms", duration.as_millis());
}

// TODO
// save as json

