use std::{process, env};
use dynamo::{Dynamo};
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

    let mut client = Dynamo::new(
        region,
        table_name,
        batch_size,
        batch_interval,
    );

    let (header, rows) = parse_csv(csv_filename);

    client.write(&header, &rows).await;
}