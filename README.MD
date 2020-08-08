### Import CSV into DynamoDB Table

##### Install:

1. Install [Rust](https://www.rust-lang.org)
2. Run `cargo install --path . ` in the repo folder


##### Usage:
- Interactive Mode
  
  - `csv_to_dynamo [filename]`
  
- Command Mode

    - `csv_to_dynamo -f [filename] -r [aws_region] -t [table_name]` 

- See help for detailed usage

    -  `csv_to_dynamo -h`

    

##### CSV Type Inference
DynamoDB table only specifies the type of primary key and sort key. For non-key attribute, this program infers the type based on the value. However, it is impossible to differentiate between primitive lists and sets, as they can have the same value (e.g. *[1, 2, 3]*). Thus, **all primitive lists will be converted to sets by default**. This conversion can be disabled via a flag (see help for more information).

Type inference examples:

* Bool: *true*
* Number: *123.456*
* String: *ABC*
* Map: *{"Name": "Joe", "Age": 15}*
* List: *["Giraffe", "Hippo" , 100]*
* String Set or List: *["Giraffe", "Hippo" ,"Zebra"]*
* Number Set or List: *[42.2, -19, 7.5, 3.14]*

