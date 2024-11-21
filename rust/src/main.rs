mod helpers;
mod collector;

use std::collections::HashMap;
use std::{thread};
use std::fmt::{Display};
use std::io::prelude::*;
use std::fs::File;
use std::ops::Index;
use std::sync::Arc;
use std::time::Instant;
use fast_float::parse;
use memmap2::Mmap;
use crate::collector::Collector;
use crate::helpers::convert_to_fixed_array;

/// Split a line on the ';' separator and parse each side, yielding
/// city_name, reading
/// Use these update the Collector for this city
fn process_line(ln: &[u8], cities: &mut HashMap<[u8; 20], Collector>) {
    let sep_index = ln.iter().position(|&byte| byte == b';').unwrap();
    let (city, reading) = (&ln[0..sep_index], &ln[sep_index + 1..]);
    let city_as_vec = convert_to_fixed_array(city);

    let temp = parse(reading).expect("Error parsing temperature");
    match cities.contains_key(&city_as_vec) {
        false => {
            cities.insert(
                city_as_vec,
                Collector::new(temp)
            );
        }
        true => {
            cities.get_mut(&city_as_vec).unwrap()
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

    let n_chars = 4096 * 200_000;
    let mut handles = vec![];

    let mmap_outer = Arc::clone(&mmap_arc);
    let (mut start_char_index, mut end_char_index) = get_next_n_chars(&mmap_outer, 0, n_chars);
    while end_char_index <= mmap_outer.len() {
        let mmap_inner = Arc::clone(&mmap_arc);

        let handle = thread::spawn(move || {
            let data_chunk = &mmap_inner[start_char_index..end_char_index];

            println!("Reading chars between {} and {}", start_char_index, end_char_index);
            let mut cities: HashMap<[u8; 20], Collector> = HashMap::with_capacity(120);

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

    let mut final_cities: HashMap<[u8; 20], Collector> = HashMap::with_capacity(120);
    for handle in handles {
        let thread_cities = handle.join().unwrap();
        thread_cities.iter().for_each(|(city, collector)| {
            match final_cities.contains_key(city) {
                true => {
                    let existing = final_cities.get(city).unwrap();
                    final_cities.insert(city.clone(), existing.add(collector.clone()));
                }
                false => {
                    final_cities.insert(city.clone(), collector.clone());
                }
            }
        })
    }

    let duration = start_time.elapsed();

    let expected = helpers::read_expected_as_hashmap();
    expected.keys().for_each(|k| {
        let city_as_str = String::from_utf8(k.to_vec()).unwrap();
        println!("Map contains city {city_as_str} with vec {:?}", k);
    });
    final_cities.iter().for_each(|(city, col)| {
        let city_as_str = String::from_utf8(city.to_vec()).unwrap();
        println!("Comparing for city {}", city_as_str);
        let matching = expected.get(city).expect(&format!("Map should contain city {city_as_str} with vec {:?}", city));
        assert_eq!(matching, &col.to_string())
    });
    println!("Elapsed time: {} ms", duration.as_millis());
}