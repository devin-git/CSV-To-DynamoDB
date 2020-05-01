use rusoto_dynamodb::{DynamoDb, DynamoDbClient, AttributeValue, BatchWriteItemInput, 
    WriteRequest, PutRequest};
use std::{thread::sleep, time::Duration};
use std::collections::HashMap;

pub struct Dynamo {
    client: DynamoDbClient,
    batch_size: usize,
    batch_interval: u64,
    table_name: String,
}

impl Dynamo {

    pub fn new(region: String, table_name: String, batch_size: i32, batch_interval: i32) -> Dynamo {
        Dynamo {
            client: DynamoDbClient::new(region.parse().unwrap()),
            batch_size: batch_size as usize,
            batch_interval: batch_interval as u64,
            table_name: table_name,
        }
    }

    // write all records into dynamoDB (multiple batches)
    pub async fn write(&self, header: &Vec<String>, rows: &Vec<Vec<String>>) {

        println!();
        println!("Batch write process started..");

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
    async fn batch_write(&self, header: &Vec<String>, rows: &Vec<&Vec<String>>) {

        let mut write_requests = Vec::new();

        for row in rows {
            // number of elements in a row must match header
            if header.len() != row.len() {
                println!("Mismatch between header and row. Row ignored: {}", row.join(" | "));
            } else {
                write_requests.push(build_write_request(header, row));
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
                    print_write_requests("Write success:", &write_requests)
                },
                Err(error) => {
                    print_write_requests("Write failure:", &write_requests);
                    println!("Error message: {}", error);
                }
            }
        }
    }
}

// build a single write request for given header and row
fn build_write_request(header: &Vec<String>, row: &Vec<String>) -> WriteRequest {
    let mut put_request = HashMap::new();

    // row must have the same length as header (check before calling this method)
    for (i, column_name) in header.iter().enumerate() {
        put_request.insert(column_name.to_owned(), guess_attr(row[i].to_owned()));
    }
    
    WriteRequest {
        put_request: Some(PutRequest{item: put_request}),
        ..Default::default()
    }
}

// print serialised write request
fn print_write_requests(text: &str, requests: &Vec<WriteRequest>) {
    for request in requests {
        // need to convert request hashmap to vector then sort by key
        let mut v: Vec<_> = request.put_request.clone().unwrap().item.into_iter().collect();
        v.sort_by(|x,y| x.0.cmp(&y.0));
        println!("{} {}", text.to_owned(), serde_json::to_string(&v).unwrap())
    } 
}

// TODO: read type from dynamoDB table, and match column name
// this is a simple heuristic method to guess the type of attribute
// it only supports Bool, Number, String
fn guess_attr(text: String) -> AttributeValue {
    let lowered_text = text.to_lowercase();

    if lowered_text == "true" || lowered_text == "false" {
        build_bool_attr(lowered_text == "true")  // Boolean
    } else if text.parse::<f64>().is_ok() {
        build_number_attr(text)   // Number
    } else {
        build_string_attr(text)   // String
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