use rusoto_dynamodb::{DynamoDb, DynamoDbClient, AttributeValue, BatchWriteItemInput, 
    DescribeTableInput, WriteRequest, PutRequest};
use bytes::{Bytes};
use std::{thread::sleep, time::Duration, fs::File, io::{BufWriter, Write} };
use std::collections::HashMap;

pub struct Dynamo {
    client: DynamoDbClient,
    batch_size: usize,
    batch_interval: u64,
    table_name: String,
    table_attrs: HashMap<String, String>,
    logger: BufWriter<File>
}

const LOG_FILE_NAME: &str = "batch_write_logs.txt";

impl Dynamo {

    pub fn new(region: String, table_name: String, batch_size: i32, batch_interval: i32) -> Dynamo {
        Dynamo {
            client: DynamoDbClient::new(region.parse().unwrap()),
            batch_size: batch_size as usize,
            batch_interval: batch_interval as u64,
            table_name: table_name,
            table_attrs: HashMap::new(),
            logger: BufWriter::new(File::create(LOG_FILE_NAME).unwrap())
        }
    }

    // write all records into dynamoDB (multiple batches)
    pub async fn write(&mut self, header: &Vec<String>, rows: &Vec<Vec<String>>) {

        println!();
        println!("Logs will be saved to {}", LOG_FILE_NAME);
        println!("Batch write process started..");

        // get attribute type definition (including only string, number and binary)
        self.table_attrs = self.get_table_attrs().await;
        let mut current_batch = Vec::new();

        for row in rows {
            current_batch.push(row);
            if current_batch.len() >= self.batch_size {
                self.batch_write(header, &current_batch).await;
                // wait for specified period 
                sleep(Duration::from_millis(self.batch_interval));
                current_batch.clear();
            }
        }
        // if there's still some rows left
        if !current_batch.is_empty() {
            self.batch_write(header, &current_batch).await;
        }

        println!("Batch write process ended..");
        println!();
    }

    // one batch write
    async fn batch_write(&mut self, header: &Vec<String>, rows: &Vec<&Vec<String>>) {

        let mut write_requests = Vec::new();

        for row in rows {
            // number of elements in a row must match header
            if header.len() != row.len() {
                println!("Mismatch between header and row. Row ignored: {}", row.join(" | "));
            } else {
                write_requests.push(build_write_request(header, row, &self.table_attrs));
            }
        }

        if !write_requests.is_empty() {

            let mut batch_items = HashMap::new();
            batch_items.insert(self.table_name.to_owned(), write_requests.clone());
        
            // this is the structure of DynamoDB BatchWriteItemInput
            let input = BatchWriteItemInput {
                request_items: batch_items,
                ..Default::default()
            };

            match self.client.batch_write_item(input).await {
                Ok(_) => {
                    self.log_requests("Write success:", &write_requests)
                },
                Err(error) => {
                    self.log_requests("Write failure:", &write_requests);
                    writeln!(self.logger, "Error message: {}", error).expect("Error: cannot save logs");
                }
            }
        }
    }

    // get attribute definition of the target table
    // so we can determine the type of each column
    async fn get_table_attrs(&self) -> HashMap<String, String> {
        let mut table_attrs = HashMap::new();

        let describe_table_input = DescribeTableInput {
            table_name: self.table_name.to_owned()
        };
    
        match self.client.describe_table(describe_table_input).await {
            Ok(table_info) => {
                let attrs = table_info.table.unwrap_or_default().attribute_definitions.unwrap_or_default();
                for attr in attrs {
                    table_attrs.insert(attr.attribute_name, attr.attribute_type);
                }
                println!("Table Definition: {}", serde_json::to_string(&table_attrs).unwrap());
            },
            Err(error) => {
                println!("Cannot read description of table: {}. {}", self.table_name, error);              
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
}

// build a single write request for given header and row
fn build_write_request(header: &Vec<String>, row: &Vec<String>, table_attrs: &HashMap<String, String>) -> WriteRequest {
    let mut put_request = HashMap::new();

    // row must have the same length as header (check before calling this method)
    for (i, column_name) in header.iter().enumerate() {
        put_request.insert(column_name.to_owned(), build_attr(table_attrs.get(column_name), row[i].to_owned()));
    }
    
    WriteRequest {
        put_request: Some(PutRequest{item: put_request}),
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
    let parsed_as_list = serde_json::from_str::<Vec<AttributeValue>>(&text);
    let parsed_as_map = serde_json::from_str::<HashMap<String, AttributeValue>>(&text);
    let parsed_as_set = serde_json::from_str::<Vec<String>>(&text);

    if parsed_as_list.is_ok() {
        // list
        build_list_attr(parsed_as_list.unwrap())

    } else if parsed_as_map.is_ok() {
        // map
        build_map_attr(parsed_as_map.unwrap())

    }  else if parsed_as_set.is_ok() {
        // can be string set or number set
        build_set_attr(parsed_as_set.unwrap())

    }  else if lowered_text == "true" || lowered_text == "false" {
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