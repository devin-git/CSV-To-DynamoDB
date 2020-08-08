### Import CSV into DynamoDB Table

##### How to build:
Run `cargo build --release` in the repo folder. 

##### Usage:
- Linux / macOS: `./csv_to_dynamo input.csv`
- Windows: `csv_to_dynamo input.csv`

##### CSV Type Inference
This program reads DynamoDB table description to help determine type of some attributes.
However, in most cases, not every column type is defined in the table description. For
columns of unknown type, it will guess the type based on data format. See examples below:
* Bool: true
* Number: 123.456
* String: ABC
* Map: {"Name": "Joe", "Age": 15}
* List: ["Giraffe", "Hippo" , 100]
* String Set or List: ["Giraffe", "Hippo" ,"Zebra"]
* Number Set or List: ["42.2", "-19", "7.5", "3.14"]

