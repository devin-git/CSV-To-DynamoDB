use std::collections::HashMap;
use chrono::{Utc, SecondsFormat};
use rusoto_core::Region;
use rusoto_dynamodb::{DynamoDb, DynamoDbClient, AttributeValue, BatchWriteItemInput, 
    WriteRequest, PutRequest};
 

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

#[tokio::main]
async fn main() {
    let client = DynamoDbClient::new(Region::ApSoutheast2);

    match client.batch_write_item(data()).await {
        Ok(_) => {
            println!("Batch write success. ")
        },
        Err(error) => {
            println!("Batch write error: {:?}", error);
        }
    }

}