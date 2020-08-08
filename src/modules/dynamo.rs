use rusoto_dynamodb::{DynamoDb, DynamoDbClient, BatchWriteItemInput, 
    DescribeTableInput, WriteRequest, PutRequest};
use std::{thread::sleep, time::Duration, fs::File, io::{BufWriter, Write}, process::exit};
use std::collections::HashMap;
use super::utility::{ProgressPrinter, read_yes_or_no};
use super::parser::Parser;

pub struct Dynamo {
    client: DynamoDbClient,
    parser: Parser,
    config: Config,
    table_attrs: HashMap<String, String>,
    logger: BufWriter<File>,
    csv_writer: BufWriter<File>,
}

#[derive(Clone)]
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
            parser: Parser::new(config.should_use_set_by_default),
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

        // save header into csv of failed items
        self.save_row_to_csv(header);

        println!("Starting to upload records:");

        let success_count = self.all_batch_write(header, rows).await;
        let error_rate = 100.0 * (rows.len() - success_count) as f64 / rows.len() as f64; 

        println!("All the records have been processed!");
        println!("Logs has been saved to {}", LOG_FILE_NAME);
        println!("Failed items has been saved to {}", FAILED_CSV_FILE_NAME);
        println!("{}/{} items has been saved in DynamoDB. Error rate: {:.2}%",
            success_count, rows.len(), error_rate);
        println!();
    }

    // preview record for user to check if type inference works as expected
    fn preview_record(&mut self, header: &Vec<String>, row: &Vec<String>) {
        let item = self.build_write_request(header, row, &self.table_attrs)
            .put_request.expect("Invalid csv: cannot parse the first record").item;
        println!("Preview the first record in DynamoDB Json format: {}", serde_json::to_string(&item).unwrap());

        if !read_yes_or_no("Does the record format look correct?", true) {
            println!("Incorrect format, exiting...");
            exit(-1);
        }

        println!();
    }

    // split all rows into batches and upload them sequentially 
    async fn all_batch_write(&mut self, header: &Vec<String>, rows: &Vec<Vec<String>>) -> usize {
        let mut current_batch = Vec::new();
        let mut success_count = 0;
        let mut progress_printer = ProgressPrinter::new(rows.len());

        for (i, row) in rows.iter().enumerate() {
            current_batch.push(row);
            progress_printer.update_progress(i + 1);
            
            if current_batch.len() >= self.config.batch_size {
                success_count += self.batch_write(header, &current_batch).await;
                if self.config.batch_interval > 0 {
                    sleep(Duration::from_millis(self.config.batch_interval));
                }
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
                write_requests.push(self.build_write_request(header, row, &self.table_attrs));
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
                    for row in rows {
                        self.save_row_to_csv(row);
                    }
                }
            }
        }

        success_count
    }

    // build a single write request for given header and row
    fn build_write_request(&self, header: &Vec<String>, row: &Vec<String>, table_attrs: &HashMap<String, String>) -> WriteRequest {
        let mut items = HashMap::new();

        // row must have the same length as header (check before calling this method)
        for (i, column_name) in header.iter().enumerate() {
            items.insert(column_name.to_owned(), self.parser.build_attr(table_attrs.get(column_name), row[i].to_owned()));
        }
        
        WriteRequest {
            put_request: Some(PutRequest{item: items}),
            ..Default::default()
        }
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

    // save a batch of requests to logs
    fn log_requests(&mut self, text: &str, requests: &Vec<WriteRequest>) {
        for request in requests {
            // convert request hashmap to vector then sort by key
            let mut v: Vec<_> = request.put_request.clone().unwrap().item.into_iter().collect();
            v.sort_by(|x,y| x.0.cmp(&y.0));
            writeln!(self.logger, "{} {}", text.to_owned(), serde_json::to_string(&v).unwrap()).expect("Error: cannot save logs.");
        } 
    }

    // save a row to csv of failed items
    // columns in the row will always be quoted
    fn save_row_to_csv(&mut self, row: &Vec<String>) {
        let mut columns = Vec::new();
        for column in row {
            // escape quotes, then add a pair of quotes at outermost layer
            columns.push(format!("\"{}\"", column.replace("\"", "\"\"")));
        }
        writeln!(self.csv_writer, "{}", columns.join(",")).expect("Error: cannot save failed items to csv.");
    }
}