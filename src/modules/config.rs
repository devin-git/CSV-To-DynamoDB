use clap::clap_app;
use std::env;
use super::utility::{read_int, read_text, read_yes_or_no, check_range};

pub struct Config {
    pub region: String, 
    pub table_name: String,
    pub batch_size: usize,
    pub batch_interval: u64,
    pub should_use_set_if_possible: bool, // convert list to set whenever possible
    pub should_preview_record: bool,
    pub enable_log: bool,
}

pub const LOG_FILE_NAME: &str = "dynamodb_logs.txt";
pub const FAILED_CSV_FILE_NAME: &str = "failed_items.csv";
pub const BATCH_SIZE_MIN: usize = 1;
pub const BATCH_SIZE_MAX: usize = 25;
pub const BATCH_SIZE_DEFAULT: &str = "10";
pub const BATCH_INTERVAL_MIN: usize = 0;
pub const BATCH_INTERVAL_MAX: usize = 30000;
pub const BATCH_INTERVAL_DEFAULT: &str = "50";


pub fn get_arguments() -> (String, Config) {
    let args: Vec<String> = env::args().collect();

    if args.len() == 2 && args[1] != "-h" && args[1] != "--help"
        && args[1] != "-V" && args[1] != "--version" {
        get_arguments_interactive_mode(args[1].to_string())
    } else {
        get_arguments_command_mode()
    }
}

fn get_arguments_command_mode() -> (String, Config) {
    let matches = clap_app!(x =>
        (name: "CSV_To_DynamoDB")
        (version: "1.0")
        (author: "Devin (github.com/devin-git)")
        (@arg FILENAME: +required "Provide CSV filename")
        (@arg REGION: -r --region +required +takes_value "Specify AWS region. E.g. ap-southeast-2, ca-central-1, eu-north-1, sa-east-1, us-west-1, cn-north-1, etc.")
        (@arg TABLE: -t --table +required +takes_value "Specify DynamoDB table name")
        (@arg BATCH_SIZE: -s --size +takes_value "Specify batch size between 1 and 25. Default 10")
        (@arg BATCH_INTERVAL: -i --interval +takes_value "Specify batch interval in milliseconds between 0 and 30000. Default 50")
        (@arg ALLOWSET: -a --allowset "Convert lists to sets whenever possible")
        (@arg PREVIEW: -p --preview "Preview the first record before uploading")
        (@arg NOLOG: -n --nolog "Do not log requests and error messages. NOT RECOMMENDED")
    )
    .get_matches();

    let config = Config {
        region:  matches.value_of("REGION")
            .unwrap()
            .to_string(),
        table_name: matches.value_of("TABLE")
            .unwrap()
            .to_string(),
        batch_size:  check_range(matches.value_of("BATCH_SIZE")
            .unwrap_or(BATCH_SIZE_DEFAULT)
            .parse()
            .expect("Error: Batch size is not a valid number"),
            BATCH_SIZE_MIN, BATCH_SIZE_MAX),
        batch_interval: check_range(matches.value_of("BATCH_INTERVAL")
            .unwrap_or(BATCH_INTERVAL_DEFAULT)
            .parse()
            .expect("Error: Batch Interval is not a valid number"),
            BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX) as u64,
        should_use_set_if_possible: matches.is_present("ALLOWSET"),
        should_preview_record: matches.is_present("PREVIEW"),
        enable_log: !matches.is_present("NOLOG"),
    };

    (matches.value_of("FILENAME").unwrap().to_string(), config)
}

fn get_arguments_interactive_mode(filename: String) -> (String, Config) {

    // initialise parameters for DynamoDB
    let region = read_text("Input Region (eg. ap-southeast-2)");
    let table_name = read_text("Input table name");
    let batch_size = read_int("Input batch size", BATCH_SIZE_MIN, BATCH_INTERVAL_MAX);
    let batch_interval = read_int("Input batch interval in milliseconds", BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX);
    let should_use_set_if_possible = read_yes_or_no("Would you like to convert list to set whenever possible?", false);
    let should_preview_record = read_yes_or_no("Would you like to preview the first record before uploading?", true);
    println!();

    (filename, Config {
        region: region,
        table_name: table_name,
        batch_size: batch_size,
        batch_interval: batch_interval as u64,
        should_use_set_if_possible: should_use_set_if_possible,
        should_preview_record: should_preview_record,
        enable_log: true,
    })
}