
use std::{ process, env};
use dynamo::{Dynamo};
use utility::{read_int, read_text, parse_csv};

mod dynamo;
mod utility;

#[tokio::main]
async fn main() {

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Please provide csv file name.");
        process::exit(-1);
    }

    // initialise parameters 
    let csv_filename = args[1].to_owned();
    let region = read_text("Input Region (eg. ap-southeast-2):");
    let table_name = read_text("Input table name:");
    let max_requests = read_int("Input max number of items per batch write (1-25):", 1, 25);
    let batch_interval = read_int("Input interval (in milliseconds) between batch write (10-5000):", 10, 5000);

    let client = Dynamo::new(
        region,
        table_name,
        max_requests,
        batch_interval,
    );

    let (header, rows) = parse_csv(csv_filename);

    client.write(&header, &rows).await;

}