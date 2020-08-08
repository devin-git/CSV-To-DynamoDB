use std::{process, env};
use dynamo::{Dynamo, Config};
use utility::{read_int, read_text, parse_csv, show_help};

mod dynamo;
mod utility;

#[tokio::main]
async fn main() {

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Please provide csv file name.");
        show_help();
        process::exit(-1);
    }

    // initialise parameters 
    let csv_filename = args[1].to_owned();
    let region = read_text("Input Region (eg. ap-southeast-2):");
    let table_name = read_text("Input table name:");
    let batch_size = read_int("Input batch size (1-25):", 1, 25);
    let batch_interval = read_int("Input batch interval in milliseconds (10-5000):", 10, 5000);
    println!();

    let mut client = Dynamo::new(
        Config {
            region: region,
            table_name: table_name,
            batch_size: batch_size as usize,
            batch_interval: batch_interval as u64,
            should_use_set_by_default: true,
            should_preview_record: true,
        }
    );

    println!("Reading csv...");
    let (header, rows) = parse_csv(csv_filename);

    if rows.len() <= 0 {
        println!("Empty csv, exiting..");
        process::exit(0);
    }

    println!("Connecting to DynamoDB...");
    client.save_all(&header, &rows).await;
}