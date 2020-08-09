use std::process::exit;
use modules::dynamo::Dynamo;
use modules::utility::parse_csv;
use modules::config::get_arguments;

mod modules;

#[tokio::main]
async fn main() {
    let (filename, config) = get_arguments();

    println!("Reading csv...");
    let (header, rows) = parse_csv(filename);

    if header.is_empty() || rows.is_empty() {
        println!("Empty csv, exiting...");
        exit(0);
    }

    let mut client = Dynamo::new(config);
    client.save_to_dynamo(&header, &rows).await;
}