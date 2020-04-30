use std::collections::HashMap;
use std::{thread, time, process, io, io::Write, env};

use chrono::{Utc, SecondsFormat};
use rusoto_core::Region;
use rusoto_dynamodb::{DynamoDb, DynamoDbClient, AttributeValue, BatchWriteItemInput, 
    WriteRequest, PutRequest};
use csv::{Reader, Error};


fn build_str_attr(text: &str) -> AttributeValue {
    AttributeValue {
        s: Some(text.to_owned()),
        ..Default::default()
    }
}

fn build_admin_role(membership_id: &str) -> WriteRequest {
    let mut put_request = HashMap::new();

    // time format: 2020-04-20T02:54:58.793Z
    let current_time = &*Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

    put_request.insert("Identifier".to_owned(), build_str_attr(membership_id));
    put_request.insert("Context".to_owned(), build_str_attr("UserRole"));
    put_request.insert("Name".to_owned(), build_str_attr("Admin"));   
    put_request.insert("CreatedDateTimeUtc".to_owned(), build_str_attr(current_time));   
    put_request.insert("LastUpdatedDateTimeUtc".to_owned(), build_str_attr(current_time));   
    
    WriteRequest {
        put_request: Some(PutRequest{item: put_request}),
        ..Default::default()
    }
}

fn data() -> BatchWriteItemInput {
    let table_name = "TradeAuth".to_owned();

    // batch generated, put membershipIds in build_admin_role
    let write_requests = vec![
        build_admin_role("some_membership_id"),
    ];

    let mut batch_items = HashMap::new();
    batch_items.insert(table_name, write_requests);

    BatchWriteItemInput {
        request_items: batch_items,
        ..Default::default()
    }
}

fn read_i32(prompt_text: &str, lower_bound: i32, upper_bound: i32) -> i32 {
    let mut n = String::new();

    print!("{}", prompt_text);
    io::stdout().flush().unwrap();

    io::stdin()
        .read_line(&mut n)
        .expect("Failed to read input.");

    let n: i32 = n.trim().parse().expect("Invalid input");

    if n < lower_bound || n > upper_bound {
        println!("Invalid input");
        process::exit(-1);
    } else {
        n
    }
}

#[tokio::main]
async fn main() {

    // print!("Input maximum write requests per batch (5-25):");
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Please provide csv file name.");
        process::exit(-1);
    }

    // initialise parameters 
    let csv_filename = &args[1];
    let max_requests = read_i32("Input max number of items per batch write (5-25):", 5, 25);
    let interval = read_i32("Input interval (in milliseconds) between batch write (100-5000):", 100, 5000);

    // read csv
    let mut reader = Reader::from_path(csv_filename).unwrap();

    let headers = reader.headers().unwrap();
    println!("{:?}", headers);
    
    for header in headers {
        println!("{:?}", header);
    }
    
    for record in reader.records() {
        let record = &record.unwrap();
        print!("{}: ", record.len());

        for x in record {
            print!("{} ", x)
        };
        // println!(
        //     "In {}, {} built the {} model. It is a {}.",
        //     &record[0],
        //     &record[1],
        //     &record[2],
        //     &record[3]
        // );
    }


    // let client = DynamoDbClient::new(Region::ApSoutheast2);

    // match client.batch_write_item(data()).await {
    //     Ok(_) => {
    //         println!("Batch write success. ")
    //     },
    //     Err(error) => {
    //         println!("Batch write error: {:?}", error);
    //     }
    // }

}