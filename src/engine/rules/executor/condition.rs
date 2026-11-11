use crate::graph::edge::{Condition, ConditionOp, Predicate};

use super::value::{self, Context};

pub(super) fn matches(
    condition: Option<&Condition>,
    context: &Context<'_>,
) -> Result<bool, crate::Error> {
    let Some(condition) = condition else {
        return Ok(true);
    };

    match condition {
        Condition::Predicate(predicate) => predicate_matches(predicate, context),
        Condition::All(group) => {
            for condition in &group.all {
                if !matches(Some(condition), context)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Condition::Any(group) => {
            for condition in &group.any {
                if matches(Some(condition), context)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn predicate_matches(predicate: &Predicate, context: &Context<'_>) -> Result<bool, crate::Error> {
    let left = value::resolve_optional_path(&predicate.from, context)?;
    match predicate.op {
        ConditionOp::Present => return Ok(left.is_some_and(|value| !value::is_empty(&value))),
        ConditionOp::Absent => return Ok(left.is_none_or(|value| value::is_empty(&value))),
        _ => {}
    }

    let Some(left) = left else {
        return Ok(false);
    };
    let Some(right) = predicate
        .value
        .as_ref()
        .map(|value| value::resolve_optional(value, context))
        .transpose()?
        .flatten()
    else {
        return Ok(false);
    };

    match predicate.op {
        ConditionOp::Eq => Ok(left == right),
        ConditionOp::Ne => Ok(left != right),
        ConditionOp::Gt | ConditionOp::Gte | ConditionOp::Lt | ConditionOp::Lte => {
            let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) else {
                return Ok(false);
            };
            Ok(match predicate.op {
                ConditionOp::Gt => left > right,
                ConditionOp::Gte => left >= right,
                ConditionOp::Lt => left < right,
                ConditionOp::Lte => left <= right,
                _ => unreachable!(),
            })
        }
        ConditionOp::Present | ConditionOp::Absent => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use serde_json::json;

    use super::*;

    fn context<'a>(
        request: &'a crate::net::Request,
        response: &'a crate::net::Response,
        fields: &'a IndexMap<String, serde_json::Value>,
        binds: &'a IndexMap<String, serde_json::Value>,
    ) -> Context<'a> {
        Context {
            request,
            response,
            fields,
            binds,
            item: None,
        }
    }

    #[test]
    fn present_accepts_false_and_zero_but_rejects_empty_values() {
        let request = crate::net::Request::follow("https://example.com").unwrap();
        let response =
            crate::net::Response::new(request.clone(), crate::net::StatusCode(200), String::new());
        let binds = IndexMap::new();

        for value in [json!(false), json!(0), json!("value"), json!([1])] {
            let fields = IndexMap::from([(String::from("value"), value)]);
            let condition: Condition = serde_json::from_value(json!({
                "from": "$fields.value",
                "op": "present"
            }))
            .unwrap();
            assert!(
                matches(
                    Some(&condition),
                    &context(&request, &response, &fields, &binds)
                )
                .unwrap()
            );
        }

        for value in [json!(null), json!(""), json!([]), json!({})] {
            let fields = IndexMap::from([(String::from("value"), value)]);
            let condition: Condition = serde_json::from_value(json!({
                "from": "$fields.value",
                "op": "present"
            }))
            .unwrap();
            assert!(
                !matches(
                    Some(&condition),
                    &context(&request, &response, &fields, &binds)
                )
                .unwrap()
            );
        }
    }

    #[test]
    fn comparisons_are_strict_and_groups_short_circuit() {
        let request = crate::net::Request::follow("https://example.com").unwrap();
        let response =
            crate::net::Response::new(request.clone(), crate::net::StatusCode(200), String::new());
        let fields = IndexMap::from([
            (String::from("has_more"), json!(true)),
            (String::from("page"), json!(2)),
        ]);
        let binds = IndexMap::new();
        let context = context(&request, &response, &fields, &binds);

        let all: Condition = serde_json::from_value(json!({
            "all": [
                {"from": "$fields.has_more", "op": "eq", "value": true},
                {"from": "$fields.page", "op": "lt", "value": 3}
            ]
        }))
        .unwrap();
        assert!(matches(Some(&all), &context).unwrap());

        let wrong_type: Condition = serde_json::from_value(json!({
            "from": "$fields.page", "op": "eq", "value": "2"
        }))
        .unwrap();
        assert!(!matches(Some(&wrong_type), &context).unwrap());

        let any: Condition = serde_json::from_value(json!({
            "any": [
                {"from": "$fields.missing", "op": "present"},
                {"from": "$fields.has_more", "op": "eq", "value": true}
            ]
        }))
        .unwrap();
        assert!(matches(Some(&any), &context).unwrap());
    }
}
