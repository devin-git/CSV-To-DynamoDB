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
    let mut text = String::new();

    print!("{}", prompt_text);
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

    let mut text = String::new();

    print!("{}", prompt_text);
    io::stdout().flush().unwrap();

    io::stdin()
    .read_line(&mut text)
    .expect("Failed to read input.");

    text.trim().to_owned()
}

pub fn show_help() {
    let help_info = r#"
Usage:
Linux or macOS: ./csv_to_dynamo input.csv
Windows: csv_to_dynamo input.csv

CSV Type Inference:
This program reads DynamoDB table description to help determine type of some attributes.
However, in most cases, not every column type is defined in the table description. For
columns of unknown type, it will guess the type based on data format. See examples below:
    * Bool: true
    * Number: 123.456
    * String: ABC
    * Map: {"Name": {"S": "Joe"}, "Age": {"N": "35"}}
    * List: [{"S": "Item1"}, {"S": "Item2"}]
    * String Set: ["Giraffe", "Hippo" ,"Zebra"]
    * Number Set: ["42.2", "-19", "7.5", "3.14"]
    "#;

    println!("{}", help_info);
}
