use std::{process::exit, env};
use modules::dynamo::{Dynamo, Config};
use modules::utility::{read_int, read_text, read_yes_or_no, parse_csv, show_help};

mod modules;

#[tokio::main]
async fn main() {

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Please provide csv file name.");
        show_help();
        exit(-1);
    }

    let csv_filename = args[1].to_owned();
    println!("Reading csv...");
    let (header, rows) = parse_csv(csv_filename);

    // initialise parameters for DynamoDB
    let region = read_text("Input Region (eg. ap-southeast-2)");
    let table_name = read_text("Input table name");
    let batch_size = read_int("Input batch size", 1, 25);
    let batch_interval = read_int("Input batch interval in milliseconds", 5, 10000);
    let should_use_set_by_default = read_yes_or_no("Would you like to convert list to set when possible?", true);
    let should_preview_record = read_yes_or_no("Would you like to preview the first record before uploading?", true);
    println!();

    let mut client = Dynamo::new(
        Config {
            region: region,
            table_name: table_name,
            batch_size: batch_size as usize,
            batch_interval: batch_interval as u64,
            should_use_set_by_default: should_use_set_by_default,
            should_preview_record: should_preview_record,
        }
    );


    if rows.is_empty() {
        println!("Empty csv, exiting..");
        exit(0);
    }

    client.save_to_dynamo(&header, &rows).await;
}