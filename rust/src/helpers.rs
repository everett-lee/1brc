use std::collections::HashMap;
use crate::Collector;

fn save_to_expected_output(final_cities: HashMap<String, Collector>) {
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