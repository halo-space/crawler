use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json_path::JsonPath;

use crate::{net, selector};

pub(crate) fn parse<T>(response: &net::Response) -> Result<T, net::Error>
where
    T: DeserializeOwned,
{
    serde_json::from_str(&response.text()?).map_err(net::Error::from)
}

pub fn select<'a>(value: &'a Value, expr: &str) -> Result<Vec<&'a Value>, selector::Error> {
    Ok(compile(expr)?.query(value).all())
}

pub(crate) fn compile(expr: &str) -> Result<JsonPath, selector::Error> {
    JsonPath::parse(expr).map_err(|error| selector::Error::Json(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eastmoney() -> Value {
        serde_json::json!({
            "rc": 0,
            "full": true,
            "dsc": null,
            "data": {
                "total": 3,
                "diff": [
                    {"f2": 6.61, "f12": "601398", "f14": "工商银行"},
                    {"f2": 46.15, "f12": "600036", "f14": "招商银行"},
                    {"f2": 6.36, "f12": "600030", "f14": "中信证券"}
                ]
            }
        })
    }

    #[test]
    fn selects_nested_values_in_document_order() {
        let value = eastmoney();
        let values = select(&value, "$.data.diff[*].f12").unwrap();

        assert_eq!(
            values.into_iter().cloned().collect::<Vec<_>>(),
            vec![
                Value::from("601398"),
                Value::from("600036"),
                Value::from("600030")
            ]
        );
    }

    #[test]
    fn preserves_objects_numbers_and_filter_results() {
        let value = eastmoney();
        let rows = select(&value, "$.data.diff[?@.f2 > 10]").unwrap();
        let total = select(&value, "$.data.total").unwrap();
        let full = select(&value, "$.full").unwrap();
        let dsc = select(&value, "$.dsc").unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["f12"], Value::from("600036"));
        assert_eq!(total, vec![&value["data"]["total"]]);
        assert_eq!(full, vec![&value["full"]]);
        assert_eq!(dsc, vec![&value["dsc"]]);
    }

    #[test]
    fn returns_empty_for_a_valid_miss() {
        let value = eastmoney();

        assert!(select(&value, "$.data.missing").unwrap().is_empty());
    }

    #[test]
    fn rejects_invalid_paths() {
        let error = select(&eastmoney(), "$.data[").unwrap_err();

        assert!(matches!(error, selector::Error::Json(_)));
    }
}
