### Import CSV into DynamoDB Table

##### How to build:
Run `cargo build --release` in the repo folder. 

##### Usage:
- Linux / macOS: `./csv_to_dynamo input.csv`
- Windows: `csv_to_dynamo input.csv`

##### CSV Type Inference
This program reads DynamoDB table description to determine the type of primary key and sort key. For non-key attributes, the type will be determined using type inference. However, it is impossible to differentiate between primitive lists and sets, as they share the same format. Thus, **all primitive lists will be converted to sets by default**. This conversion can be disabled via a flag (see help for more information).

Type inference examples:

* Bool: true
* Number: 123.456
* String: ABC
* Map: {"Name": "Joe", "Age": 15}
* List: ["Giraffe", "Hippo" , 100]
* String Set or List: ["Giraffe", "Hippo" ,"Zebra"]
* Number Set or List: ["42.2", "-19", "7.5", "3.14"]

