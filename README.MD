## Import CSV into DynamoDB

#### Features:

- Support complex data (infinitely nested maps and lists)
- Control throughput via batch size and interval
- Save failed records into csv
- Save all requests and error messages into log

#### Manual Install:

1. Install [Rust](https://www.rust-lang.org)
2. Clone the repo
3. Run `cargo install --path . ` in the repo folder


#### Usage:
- Interactive Mode
  
  - `csv_to_dynamo [filename]`
  
- Command Mode

    - `csv_to_dynamo [filename] -r [aws_region] -t [table_name]` 

- Check Detailed Usage

    - `csv_to_dynamo -h`

        
    

##### Note: all the fields in csv must be quoted and comma-delimited (see examples folder)



#### CSV Type Inference

DynamoDB table only specifies the type of primary key and sort key. For non-key attributes, this program infers the type based on the value. However, it is impossible to differentiate between primitive lists and sets, as they can have the same value (e.g. *[1, 2, 3]*). Thus, **all primitive lists will be converted to sets by default** (except list of duplicates and empty list). This conversion can be disabled via a flag (see help for more information).

Supported formats:

* Bool: *true*
* Number: *123.456*
* String: *ABC*
* Map: *{"Name": "Joe", "Age": 15}*
* List: *["Giraffe", "Hippo" , 100]*
* String Set or List: *["Giraffe", "Hippo" ,"Zebra"]*
* Number Set or List: *[42.2, -19, 7.5, 3.14]*
* Nested Data: [{"Name": "Joe", "Age": 15}, {"Subjects": [ {"Id": 1, "IsActive": true, "Scores": [10, 20, 30]}]}] 



