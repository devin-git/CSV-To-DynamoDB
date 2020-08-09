use serde_json::{Value, to_string, from_str};
use rusoto_dynamodb::AttributeValue;
use std::{collections::HashMap};
use bytes::Bytes;
use itertools::Itertools;

pub struct Parser {
    should_use_set_if_possible: bool
}

impl Parser {

    pub fn new(should_use_set_if_possible: bool) -> Parser {
        Parser {
            should_use_set_if_possible: should_use_set_if_possible
        }
    }

    pub fn build_attr(&self, column_type: Option<&String>, text: String) -> AttributeValue {
        match column_type {
            // type is known, key attribute
            Some(some_type) => {
                match some_type.as_str() {
                    // type is number
                    "N" => build_number_attr(text),
    
                    // type is byte
                    "B" => build_bytes_attr(Bytes::from(text)),
    
                    // type is string
                    "S" => build_string_attr(text),
    
                    // in theory, we won't get other type for key
                    _ => self.parse_string_as_attr(text),
                }
            },
            None => {
                // type is unknown, non-key attribute
                self.parse_string_as_attr(text) 
            }
        }
    }
    
    // try to parse the string as different types of attribute
    // order: null, bool, number, json (complex value), string
    fn parse_string_as_attr(&self, text: String) -> AttributeValue {
    
        let parsed_as_null = text == "null";
        let parsed_as_number = text.parse::<f64>();
        let parsed_as_bool = text.parse::<bool>();
        let parsed_as_json_value = from_str::<Value>(&text);
    
        if parsed_as_null {
            build_null_attr()
        } else if parsed_as_number.is_ok() {
            build_number_attr(text)
        } else if parsed_as_bool.is_ok() {
            build_bool_attr(parsed_as_bool.unwrap())
        } else if parsed_as_json_value.is_ok() {
            self.parse_json_as_attr(parsed_as_json_value.unwrap())
        } else {
            build_string_attr(text)
        }
    }
    
    fn parse_json_as_attr(&self, json: Value) -> AttributeValue {
    
        match json {
    
            Value::Null => {
                // this is inside list or map, use empty string instead of null
                build_empty_string_attr()
            }
    
            Value::Bool(x) => {
                build_bool_attr(x)
            }
    
            Value::Number(x) => {
                build_number_attr(to_string(&x).unwrap())
            }
    
            Value::String(x) => {
                build_string_attr(x)
            }
    
            Value::Array(array) => {
                let array_type = self.parse_json_array_type(&array);
                match array_type {
                    ArrayType::List => build_list_attr(array.into_iter().map(|x| self.parse_json_as_attr(x)).collect()),
                    ArrayType::StringSet => build_string_set_attr(array.into_iter().map(|x| x.as_str().unwrap().to_string()).collect()),
                    ArrayType::NumberSet => build_number_set_attr(array.into_iter().map(|x| x.as_str().unwrap().to_string()).collect()),
                }
            }

            Value::Object(dictionary) => {
                let mut attr_map = HashMap::new();
                for (k, v) in dictionary {
                    attr_map.insert(k, self.parse_json_as_attr(v));
                }
                build_map_attr(attr_map)
            }
        }
    }

    // a list in the json can be either List or Set in dynamodb
    // this method takes into account should_use_set_if_possible 
    fn parse_json_array_type(&self, list: &Vec<Value>) -> ArrayType {
        if !self.should_use_set_if_possible {
            ArrayType::List
        } else {
            // set cannot be empty in dynamodb
            if list.is_empty() {
                ArrayType::List 
            } else {
                if is_string_set(&list) {
                    ArrayType::StringSet
                } else if is_number_set(&list) {
                    ArrayType::NumberSet
                } else {
                    ArrayType::List
                }
            }
        }
    }
}

enum ArrayType {
    List,
    NumberSet,
    StringSet
}

// check if all itmes in the list is unique string
fn is_string_set(list: &Vec<Value>) -> bool{
    let str_list: Vec<_> = list.iter().map(|x| x.as_str()).collect();
    str_list.iter().all(|x| x.is_some()) && list.len() == str_list.iter().unique().count()
}

// check if all itmes in the list is unique number
fn is_number_set(list: &Vec<Value>) -> bool {
    let f64_list: Vec<_> = list.iter().map(|x| x.as_f64()).collect();
    if f64_list.iter().all(|x| x.is_some()) {
        let num_as_string_list: Vec<_> = list.iter().map(|x| to_string(x).unwrap_or_default()).collect();
        list.len() == num_as_string_list.iter().unique().count()
    } else {
        false
    }
}

// this should only be used at top level
// it cannot be used inside map or list
fn build_null_attr() -> AttributeValue {
    AttributeValue {
        s: None,
        ..Default::default()
    }
}

fn build_empty_string_attr() -> AttributeValue {
    build_string_attr("".to_string())
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

fn build_bytes_attr(b: Bytes) -> AttributeValue {
    AttributeValue {
        b: Some(b),
        ..Default::default()
    }
}

fn build_list_attr(list: Vec<AttributeValue>) -> AttributeValue {
    AttributeValue {
        l: Some(list),
        ..Default::default()
    }
}

fn build_string_set_attr(list: Vec<String>) -> AttributeValue {
    AttributeValue {
        ss: Some(list),
        ..Default::default()
    }
}

fn build_number_set_attr(list: Vec<String>) -> AttributeValue {
    AttributeValue {
        ns: Some(list),
        ..Default::default()
    }
}

fn build_map_attr(map: HashMap<String, AttributeValue>) -> AttributeValue {
    AttributeValue {
        m: Some(map),
        ..Default::default()
    }
}