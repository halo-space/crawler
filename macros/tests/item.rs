use spider::item::{Item as _, Values};

#[derive(Debug, serde::Deserialize, serde::Serialize, macros::Item)]
#[serde(deny_unknown_fields)]
struct Article {
    title: String,
    #[serde(skip)]
    state: spider::item::State,
}

#[derive(serde::Deserialize, serde::Serialize, macros::Item)]
#[serde(deny_unknown_fields)]
struct Record<T> {
    value: T,
    #[serde(skip)]
    state: spider::item::State,
}

#[test]
fn derives_the_complete_item_contract() {
    let mut item = Article::from_values(Values::from([(
        "title".to_string(),
        serde_json::Value::from("Rust"),
    )]))
    .unwrap();

    assert_eq!(item.title, "Rust");
    assert_eq!(item.state().id(), "");
    *item.id_mut() = "item-1".to_string();
    assert_eq!(item.id(), "item-1");
    assert_eq!(
        serde_json::to_value(&item).unwrap(),
        serde_json::json!({"title": "Rust"})
    );

    let mut item: Box<dyn spider::item::Item> = Box::new(item);
    assert_eq!(item.downcast_ref::<Article>().unwrap().title, "Rust");
    item.downcast_mut::<Article>().unwrap().title = "Crawler".to_string();
    assert_eq!(item.downcast_ref::<Article>().unwrap().title, "Crawler");
}

#[test]
fn rejects_values_that_are_not_item_fields() {
    let error = Article::from_values(Values::from([
        ("title".to_string(), serde_json::Value::from("Rust")),
        ("titel".to_string(), serde_json::Value::from("typo")),
    ]))
    .unwrap_err();

    assert!(matches!(&error, spider::item::Error::Deserialize(_)));
    assert!(error.to_string().contains("unknown field `titel`"));
    assert!(std::error::Error::source(&error).is_some());
}

#[test]
fn rejects_missing_and_invalid_business_fields() {
    let missing = Article::from_values(Values::new()).unwrap_err();
    let invalid = Article::from_values(Values::from([(
        "title".to_string(),
        serde_json::Value::from(7),
    )]))
    .unwrap_err();

    assert!(matches!(missing, spider::item::Error::Deserialize(_)));
    assert!(matches!(invalid, spider::item::Error::Deserialize(_)));
}

#[test]
fn preserves_generic_item_bounds() {
    let item = Record::<String>::from_values(Values::from([(
        "value".to_string(),
        serde_json::Value::from("typed"),
    )]))
    .unwrap();

    assert_eq!(item.value, "typed");
}
