use halo::harbor::item::{Item as _, Values};

#[derive(Debug, serde::Deserialize, serde::Serialize, macros::Item)]
#[serde(deny_unknown_fields)]
struct Article {
    title: String,
    #[serde(skip)]
    state: halo::harbor::item::State,
}

#[test]
fn public_facade_supports_the_item_macro_contract() {
    let mut item = Article::from_values(Values::from([(
        "title".to_string(),
        serde_json::Value::from("Rust"),
    )]))
    .unwrap();

    assert_eq!(item.title, "Rust");
    assert_eq!(item.state().id(), "");
    *item.id_mut() = "item-1".to_string();
    assert_eq!(item.id(), "item-1");
}
