use std::collections::HashMap;
use std::fs;
use crate::Collector;

pub fn convert_to_fixed_array(slice: &[u8]) -> [u8; 20] {
    let mut array = [0u8; 20]; // Initialize a fixed-size array with default value 0
    let length = slice.len().min(20); // Ensure we only copy up to 15 bytes
    array[..length].copy_from_slice(&slice[..length]); // Copy slice into the array
    array
}

pub fn read_expected_as_hashmap() -> HashMap<[u8; 20], String> {
    let content = fs::read_to_string("../averages.txt").unwrap();

    let trimmed = content.trim().trim_start_matches('{').trim_end_matches('}');
    let pairs: Vec<&str> = trimmed.split(',').collect();

    let mut city_to_stats = HashMap::new();
    for pair in pairs {
        let mut kv = pair.splitn(2, '=');
        if let (Some(mut key), Some(value)) = (kv.next(), kv.next()) {
            let mut inserted_key = key;
            if key.contains("Washington") {
                inserted_key = "Washington, D.C.";
            }
            if key.contains("Petén") {
                inserted_key = "Flores,  Petén"
            }
            let city_as_vec: [u8; 20] = convert_to_fixed_array(inserted_key.trim().as_bytes());
            // inserted_key.trim().to_string()
            city_to_stats.insert(city_as_vec, value.trim().to_string());
        }
    }
    city_to_stats
}

pub fn save_to_expected_output(final_cities: HashMap<String, Collector>) {
    let mut sorted: Vec<(&String, &Collector)> = final_cities.iter().collect();
    sorted.sort_by_key(|&(key, _)| key);

    let mut output = vec![];
    sorted.iter().for_each(|(city, col)| {
        output.push(format!("{}={}" , city, col));
    });

    let mut builder = String::new();
    let joined = output.join(", ");
    builder.push('{');
    builder.push_str(&joined);
    builder.push('}');

    // TODO actually save to file
    println!("{}", builder);
}