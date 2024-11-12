use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone)]
pub struct Collector {
    min: f32,
    max: f32,
    count: i32,
    sum: f32,
}

impl Collector {
    pub fn new(starting_val: f32) -> Collector {
        Collector {
            min: starting_val,
            max: starting_val,
            count: 1,
            sum: starting_val,
        }
    }

    pub fn add(&self, other: Collector) -> Collector {
        Collector {
            min: f32::min(self.min, other.min),
            max: f32::max(self.max, other.max),
            count: self.count + other.count,
            sum: self.sum + other.sum,
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
            f,
            "{:.1}/{:.1}/{:.1}",
            self.min,
            ((self.sum / self.count as f32) * 10.0).round() / 10.0,
            self.max
        )
    }
}
