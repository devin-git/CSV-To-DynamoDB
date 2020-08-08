use serde_json::Value;
use rusoto_dynamodb::{DynamoDb, DynamoDbClient, AttributeValue, BatchWriteItemInput, 
    DescribeTableInput, WriteRequest, PutRequest};
use bytes::{Bytes};
use std::{thread::sleep, time::Duration, fs::File, io::{BufWriter, Write}, process::exit};
use std::collections::HashMap;
use super::utility::{Progress_Printer, read_yes_or_no};

pub struct Dynamo {
    client: DynamoDbClient,
    config: Config,
    table_attrs: HashMap<String, String>,
    logger: BufWriter<File>,
    csv_writer: BufWriter<File>,
}

pub struct Config {
    pub region: String, 
    pub table_name: String,
    pub batch_size: usize,
    pub batch_interval: u64,
    pub should_use_set_by_default: bool, // convert list to set in dynamodb when possible
    pub should_preview_record: bool,
}

const LOG_FILE_NAME: &str = "batch_write_logs.txt";
const FAILED_CSV_FILE_NAME: &str = "failed_items.csv";

impl Dynamo {

    pub fn new(config: Config) -> Dynamo {
        Dynamo {
            client: DynamoDbClient::new(config.region.parse().unwrap()),
            config: config,
            table_attrs: HashMap::new(),
            logger: BufWriter::new(File::create(LOG_FILE_NAME).unwrap()),
            csv_writer: BufWriter::new(File::create(FAILED_CSV_FILE_NAME).unwrap()),
        }
    }

    // save all records into dynamoDB (multiple batches)
    pub async fn save_to_dynamo(&mut self, header: &Vec<String>, rows: &Vec<Vec<String>>) {

        // preview first record to check if type inference works as expected
        if self.config.should_preview_record {
            self.preview_record(header, &rows[0]);
        }

        // get table definition (type of primary key/sort key)
        self.table_attrs = self.get_table_attrs().await;

        println!("Starting to upload records:");

        let success_count = self.all_batch_write(header, rows).await;
        let error_rate = 100.0 * (rows.len() - success_count) as f64 / rows.len() as f64; 

        println!("All the records have been processed.");
        println!("Logs has been saved to {}", LOG_FILE_NAME);
        println!("{}/{} items has been saved in DynamoDB. Error rate:{:.2}%",
            success_count, rows.len(), error_rate);
        println!();
    }

    // preview record for user to check if type inference works as expected
    pub fn preview_record(&mut self, header: &Vec<String>, row: &Vec<String>) {
        print!("Preview the first record in DynamoDB Json:");

        let item = build_write_request(header, row, &self.table_attrs).put_request.unwrap().item;
        println!("{}", serde_json::to_string(&item).unwrap());

        if !read_yes_or_no("Does the record format look correct?", true) {
            println!("Incorrect format, exiting...");
            exit(-1);
        }

        println!();
    }

    // split all rows into batches and upload them sequentially 
    pub async fn all_batch_write(&mut self, header: &Vec<String>, rows: &Vec<Vec<String>>) -> usize {
        let mut current_batch = Vec::new();
        let mut success_count = 0;
        let mut progress_printer = Progress_Printer::new(rows.len());

        for (i, row) in rows.iter().enumerate() {
            current_batch.push(row);
            progress_printer.update_progress(i + 1);
            if current_batch.len() >= self.config.batch_size {
                success_count += self.batch_write(header, &current_batch).await;
                // wait for specified period 
                sleep(Duration::from_millis(self.config.batch_interval));
                current_batch.clear();
            }
        }
        
        // if there's still some rows left
        if !current_batch.is_empty() {
            success_count += self.batch_write(header, &current_batch).await;
        }

        success_count
    }

    // one batch write, 25 rows at most
    async fn batch_write(&mut self, header: &Vec<String>, rows: &Vec<&Vec<String>>) -> usize {
        let mut write_requests = Vec::new();
        let mut success_count = 0;

        for row in rows {
            if header.len() != row.len() {
                println!("Mismatch between header and row. Row ignored: {}", row.join(" | "));
            } else {
                write_requests.push(build_write_request(header, row, &self.table_attrs));
            }
        }

        if !write_requests.is_empty() {
            let mut batch_items = HashMap::new();
            batch_items.insert(self.config.table_name.to_owned(), write_requests.clone());
        
            // this is the structure of DynamoDB BatchWriteItemInput
            let input = BatchWriteItemInput {
                request_items: batch_items,
                ..Default::default()
            };

            match self.client.batch_write_item(input).await {
                Ok(_) => {
                    self.log_requests("Write success:", &write_requests);
                    success_count += rows.len();
                },
                Err(error) => {
                    self.log_requests("Write failure:", &write_requests);
                    writeln!(self.logger, "Error message: {}", error).expect("Error: cannot save logs");
                }
            }
        }

        success_count
    }

    // get attribute definition of the target table
    // we can only get type of primary key / sort key
    async fn get_table_attrs(&self) -> HashMap<String, String> {
        println!("Reading DynamoDB table definition...");

        let mut table_attrs = HashMap::new();
        let describe_table_input = DescribeTableInput {
            table_name: self.config.table_name.to_owned()
        };
    
        match self.client.describe_table(describe_table_input).await {
            Ok(table_info) => {
                let attrs = table_info.table.unwrap_or_default().attribute_definitions.unwrap_or_default();
                for attr in attrs {
                    table_attrs.insert(attr.attribute_name, attr.attribute_type);
                }
                println!("{} table definition: {}", self.config.table_name, serde_json::to_string(&table_attrs).unwrap());
            },
            Err(error) => {
                println!("Cannot read description of table: {}. {}", self.config.table_name, error);              
            }
        }
        table_attrs
    }

    // write logs to LOG_FILE_NAME
    fn log_requests(&mut self, text: &str, requests: &Vec<WriteRequest>) {
        for request in requests {
            // need to convert request hashmap to vector then sort by key
            let mut v: Vec<_> = request.put_request.clone().unwrap().item.into_iter().collect();
            v.sort_by(|x,y| x.0.cmp(&y.0));
            writeln!(self.logger, "{} {}", text.to_owned(), serde_json::to_string(&v).unwrap()).expect("Error: cannot save logs.");
        } 
    }

    // save a row to csv of failed items
    fn save_row_to_csv(&mut self, row: &Vec<String>) {
        
    }
}

// build a single write request for given header and row
fn build_write_request(header: &Vec<String>, row: &Vec<String>, table_attrs: &HashMap<String, String>) -> WriteRequest {
    let mut items = HashMap::new();

    // row must have the same length as header (check before calling this method)
    for (i, column_name) in header.iter().enumerate() {
        items.insert(column_name.to_owned(), build_attr(table_attrs.get(column_name), row[i].to_owned()));
    }
    
    WriteRequest {
        put_request: Some(PutRequest{item: items}),
        ..Default::default()
    }
}

// print serialised write request
// fn print_write_requests(text: &str, requests: &Vec<WriteRequest>) {
//     for request in requests {
//         // need to convert request hashmap to vector then sort by key
//         let mut v: Vec<_> = request.put_request.clone().unwrap().item.into_iter().collect();
//         v.sort_by(|x,y| x.0.cmp(&y.0));
//         println!("{} {}", text.to_owned(), serde_json::to_string(&v).unwrap())
//     } 
// }

fn build_attr(column_type: Option<&String>, text: String) -> AttributeValue {
    match column_type {
        Some(some_type) => {
            match some_type.as_str() {
                // type is number
                "N" => build_number_attr(text),

                // type is byte
                "B" => build_bytes_attr(Bytes::from(text)),

                // type is string
                "S" => build_string_attr(text),

                // in theory, we won't get any type other than "NBS"
                _ => guess_attr(text),
            }
        },
        None => {
            // type is unknown
            guess_attr(text) 
        }
    }
}

// a simple heuristic method to guess the type of attribute
// supports Bool, Number, String, List, Map, Number Set, String Set
fn guess_attr(text: String) -> AttributeValue {
    let lowered_text = text.to_lowercase();
    // let luck = serde_json::from_str::<Vec<String>>(&text);
    let parsed_as_list_map = serde_json::from_str::<Vec<HashMap<String, String>>>(&text);
    let parsed_as_map = serde_json::from_str::<HashMap<String, AttributeValue>>(&text);
    let parsed_as_set = serde_json::from_str::<Vec<String>>(&text);
    let parsed_as_list = serde_json::from_str::<Vec<AttributeValue>>(&text);

    if parsed_as_list_map.is_ok() {
        build_list_map_attr(parsed_as_list_map.unwrap())
    } else if parsed_as_map.is_ok() {
        // map
        build_map_attr(parsed_as_map.unwrap())

    }  else if parsed_as_set.is_ok() {
        // can be string set or number set
        build_set_attr(parsed_as_set.unwrap())

    }  else if parsed_as_list.is_ok() {
        // list
        build_list_attr(parsed_as_list.unwrap())

    } else if lowered_text == "true" || lowered_text == "false" {
        // boolean
        build_bool_attr(lowered_text == "true")

    } else if text.parse::<f64>().is_ok() {
        // number
        build_number_attr(text)

    } else {
        // string
        build_string_attr(text)
    }
}

fn build_string_attr(text: String) -> AttributeValue {
    AttributeValue {
        s: Some(text),
        ..Default::default()
    }
}

fn build_bool_attr(b: bool) -> AttributeValue {
    AttributeValue {
        bool: Some(b),
        ..Default::default()
    }
}

fn build_number_attr(text: String) -> AttributeValue {
    AttributeValue {
        n: Some(text),
        ..Default::default()
    }
}

fn build_bytes_attr(b: Bytes) -> AttributeValue {
    AttributeValue {
        b: Some(b),
        ..Default::default()
    }
}

fn build_list_attr(list: Vec<AttributeValue>) -> AttributeValue {
    AttributeValue {
        l: Some(list),
        ..Default::default()
    }
}

fn build_list_map_attr(list: Vec<HashMap<String, String>>) -> AttributeValue {
    let mut maps = Vec::new();
    for x in list {
        maps.push(build_map_string_attr(x));
    }
    AttributeValue {
        l: Some(maps),
        ..Default::default()
    }
}

fn build_map_string_attr(map: HashMap<String, String>) -> AttributeValue {
    let mut new_map = HashMap::new();

    for (k, v) in map.iter() {
        new_map.insert(k.to_string(), guess_attr(v.to_string()));
    }

    AttributeValue {
        m: Some(new_map),
        ..Default::default()
    }
}

fn build_map_attr(map: HashMap<String, AttributeValue>) -> AttributeValue {
    AttributeValue {
        m: Some(map),
        ..Default::default()
    }
}

// determine if it's number set or string set, then build it
fn build_set_attr(set: Vec<String>) -> AttributeValue {
    let mut is_number_set = true;

    for x in &set {
        if x.parse::<f64>().is_err() {
            is_number_set = false;
            break;
        }
    }

    if is_number_set {
        // it should be number set, as every element can be parsed as float
        AttributeValue {
            ns: Some(set),
            ..Default::default()
        }
    } else {
        // string set
        AttributeValue {
            ss: Some(set),
            ..Default::default()
        }
    }
}