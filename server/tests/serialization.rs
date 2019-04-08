use serde_json::to_string;

use declarative_dataflow::{Uuid, Value};
use Value::{Aid, Bool, Instant, Number, String};

#[test]
fn test_serialization() {
    assert_eq!(
        to_string(&Aid(":edge".to_string())).unwrap(),
        "{\"Aid\":\":edge\"}".to_string()
    );
    assert_eq!(
        to_string(&String("foo".to_string())).unwrap(),
        "\"foo\"".to_string()
    );
    assert_eq!(to_string(&Bool(true)).unwrap(), "true".to_string());
    assert_eq!(to_string(&Number(44)).unwrap(), "44".to_string());
    assert_eq!(
        to_string(&Instant(0)).unwrap(),
        "{\"Instant\":0}".to_string()
    );

    let uuid = Value::Uuid(Uuid::parse_str("71828aae-4fc8-421b-82ca-68c5f4981d74").unwrap());
    assert_eq!(
        to_string(&uuid).unwrap(),
        "{\"Uuid\":\"71828aae-4fc8-421b-82ca-68c5f4981d74\"}".to_string(),
    );
}
