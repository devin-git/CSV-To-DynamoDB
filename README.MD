# Import CSV into DynamoDB

## Features

- Support complex data types (JSON)
- Control throughput via batch size and interval
- Save failed records into csv
- Save all requests and error messages into log

## Download
- [Latest](https://github.com/devin-git/CSV-To-DynamoDB/releases) 

## Install From Source

1. Install [Rust](https://www.rust-lang.org)
2. Clone the repo
3. Run `cargo install --path . ` in the repo folder


## Usage
- Interactive Mode
  
  - `csv_to_dynamo [filename]`
  
- Command Mode

    - `csv_to_dynamo [filename] -r [aws_region] -t [table_name]` 

- Check Detailed Usage

    - `csv_to_dynamo -h`

##### Note: all the fields in csv must be quoted and comma-delimited (see examples folder)



## Type Inference

DynamoDB table description only specifies the type of primary key and sort key. For non-key attributes, this program infers the type based on the value. However, it is impossible to differentiate between primitive lists and sets, as they have the same format. A flag `--allowset` is introduced to handle the ambiguity. When the flag is enabled, all primitive lists will be converted to sets if possible (not empty, no duplicates). `--allowset` is **disabled by default**, in which case the program will always use lists.

##### Supported Data Types:

* Bool: *true*
* Number: *123.456*
* String: *ABC*
* Map: *{"Name": "Joe", "Age": 15}*
* List: *["Giraffe", "Hippo" , 100]*
* String Set or List: *["Giraffe", "Hippo" ,"Zebra"]*
* Number Set or List: *[42.2, -19, 7.5, 3.14]*
* JSON: *[{"Name": "Joe", "Age": 15}, {"Subjects": [ {"Id": 1, "IsActive": true, "Scores": [10, 20, 30]}]}]*

## License

This project is distributed under the terms of the MIT license.

See [LICENSE](https://github.com/devin-git/CSV-To-DynamoDB/blob/master/LICENSE) for details.

