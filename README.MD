### import CSV into DynamoDB Table

##### Usage:
./batch_write input.csv

##### CSV Type Inference
This program reads DynamoDB table description to help determine type of some attributes.
However, in most cases, not every column type is defined in the table description. For
columns of unknown type, it will guess the type based on data format. See examples below:
* Bool: true
* Number: 123.456
* String: ABC
* Map: {"Name": {"S": "Joe"}, "Age": {"N": "35"}}
* List: [{"S": "Item1"}, {"S": "Item2"}]
* String Set: ["Giraffe", "Hippo" ,"Zebra"]
* Number Set: ["42.2", "-19", "7.5", "3.14"]

