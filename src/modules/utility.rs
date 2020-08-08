use std::{process, io, io::Write};
use csv::{Reader};

// read csv, return header and content (in two different vecs)
pub fn parse_csv(filename: String) -> (Vec<String>, Vec<Vec<String>>) {
    
    let mut header_vec = Vec::new();
    let mut rows_vec = Vec::new();
    let mut reader = Reader::from_path(filename).expect("Cannot properly read csv file.");
    let headers = reader.headers().expect("Invalid csv header.");
    
    for header in headers {
        header_vec.push(header.to_owned());
    }

    for record in reader.records() {
        let row = &record.expect("Invalid csv format. Please ensure each row matches the header definition.");
        let mut row_vec = Vec::new();

        for column in row {
            row_vec.push(column.to_owned())
        };

        rows_vec.push(row_vec);
    }

    (header_vec, rows_vec)
}

// read an integer, given specified range
pub fn read_int(prompt_text: &str, lower_bound: i32, upper_bound: i32) -> i32 {

    print!("{} ({}-{}):", prompt_text, lower_bound, upper_bound);

    let mut text = String::new();
    io::stdout().flush().unwrap();
    io::stdin()
        .read_line(&mut text)
        .expect("Failed to read input.");

    let n: i32 = text.trim().parse().expect("Invalid input");

    if n < lower_bound || n > upper_bound {
        println!("Invalid input: {} is not between {} and {}.", n, lower_bound, upper_bound);
        process::exit(-1);
    } else {
        n
    }
}

// read a string
pub fn read_text(prompt_text: &str) -> String {

    print!("{}:", prompt_text);

    let mut text = String::new();
    io::stdout().flush().unwrap();
    io::stdin()
        .read_line(&mut text)
        .expect("Failed to read input.");

    text.trim().to_owned()
}

// read a boolean (yes or no)
pub fn read_yes_or_no(prompt_text: &str, default: bool) -> bool {

    if default {
        print!("{} (Y/n):", prompt_text);
    }
    else {
        print!("{} (y/N):", prompt_text);
    }

    let mut text = String::new();
    io::stdout().flush().unwrap();
    io::stdin()
        .read_line(&mut text)
        .expect("Failed to read input.");

    let answer = text.trim().to_lowercase();

    if answer.len() < 1 {
        default
    } else {
        answer.chars().nth(0).unwrap() == 'y'
    }
}

pub struct ProgressPrinter {
    current_percentage: usize,
    total_count: usize,
}

// helper to print progress
// example of 12%
// ==========:10%
// ==
impl ProgressPrinter {
    pub fn new(total_count: usize) -> ProgressPrinter {
        ProgressPrinter {
            current_percentage: 0,
            total_count: total_count
        }
    }

    pub fn update_progress(&mut self, updated_count: usize) {
        let updated_percentage = (100.0 * updated_count as f64 / self.total_count as f64).floor() as usize;

        while self.current_percentage < updated_percentage && self.current_percentage <= 100 {
            self.current_percentage += 1;
            print!("=");
            if self.current_percentage % 10 == 0 {
                println!(":{}%", self.current_percentage);
            }
        } 
    }
}

pub fn show_help() {
    let help_info = r#"
Usage:
Linux or macOS: ./csv_to_dynamo input.csv
Windows: csv_to_dynamo input.csv

CSV Type Inference:
This program reads DynamoDB table description to determine the type of primary key and sort key.
For non-key attributes, the type will be determined using type inference. 
However, it is impossible to differentiate between primitive lists and sets, as they share the same format. 
Thus, all primitive lists will be converted to sets by default. This conversion can be disabled via a flag.
Inference examples:
    * Bool: true
    * Number: 123.456
    * String: ABC
    * Map: {"Name": "Joe", "Age": 15}
    * List: ["Giraffe", "Hippo" , 100]
    * String Set or List: ["Giraffe", "Hippo" ,"Zebra"]
    * Number Set or List: ["42.2", "-19", "7.5", "3.14"]
    "#;

    println!("{}", help_info);
}
