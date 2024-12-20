use std::collections::HashMap;
use std::fs;
use crate::Collector;

pub fn convert_to_fixed_array(slice: &[u8]) -> [u8; 20] {
    let mut array = [0u8; 20];
    let length = slice.len().min(20);
    array[..length].copy_from_slice(&slice[..length]);
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

pub fn save_to_expected_output(final_cities: &HashMap<[u8;20], Collector>) {
    let mut sorted: Vec<(String, &Collector)> = final_cities.iter()
        .map(|(k, v)| {
            let last_zero_index = k.iter().position(|&c| c == 0);
            let as_str = match last_zero_index {
                None => String::from_utf8(k.to_vec()).unwrap(),
                Some(index) =>  String::from_utf8(k[..index].to_vec()).unwrap()
            };
            (as_str, v)
        })
        .collect();

    sorted.sort_by_key(|city| city.0.clone());

    let mut output = vec![];
    sorted.iter().for_each(|(city, col)| {
        output.push(format!("{},{}" , city, col.comma_separated_line()));
    });

    let joined = output.join("\n");
    fs::write("averages-rust.csv", joined).unwrap();
}