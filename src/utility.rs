use std::{process, io, io::Write};
use csv::{Reader};


// read csv, return header and content (in two different vecs)
pub fn parse_csv(filename: String) -> (Vec<String>, Vec<Vec<String>>) {
    
    let mut header_vec = Vec::new();
    let mut rows_vec = Vec::new();
    let mut reader = Reader::from_path(filename).unwrap();
    let headers = reader.headers().unwrap();
    
    for header in headers {
        header_vec.push(header.to_owned());
    }

    for record in reader.records() {
        let row = &record.unwrap();
        let mut row_vec = Vec::new();

        for column in row {
            row_vec.push(column.to_owned())
        };

        rows_vec.push(row_vec);
    }

    (header_vec, rows_vec)
}

pub fn read_int(prompt_text: &str, lower_bound: i32, upper_bound: i32) -> i32 {
    let mut text = String::new();

    print!("{}", prompt_text);
    io::stdout().flush().unwrap();

    io::stdin()
        .read_line(&mut text)
        .expect("Failed to read input.");

    let n: i32 = text.trim().parse().expect("Invalid input");

    if n < lower_bound || n > upper_bound {
        println!("Invalid input");
        process::exit(-1);
    } else {
        n
    }
}

pub fn read_text(prompt_text: &str) -> String {

    let mut text = String::new();

    print!("{}", prompt_text);
    io::stdout().flush().unwrap();

    io::stdin()
    .read_line(&mut text)
    .expect("Failed to read input.");

    text.trim().to_owned()
}