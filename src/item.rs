pub mod attachment;
pub mod config;
mod contract;
pub mod function;
mod jsonl;
pub mod map;
pub(crate) mod media;
pub mod schema;
mod state;
mod store;

pub use crate::error::item::Error;
pub use config::Config;
pub use contract::Item;
pub use function::Function;
pub use jsonl::Jsonl;
pub use map::Map;
pub use schema::SchemaKey;
pub use state::State;
pub use store::Store;

pub type Values = indexmap::IndexMap<String, serde_json::Value>;

pub(crate) fn middleware_specs(item: &dyn Item) -> Vec<crate::middleware::Spec> {
    let state = item.state().middlewares();
    let declared = item.middlewares();
    if std::ptr::eq(state, declared) {
        state.to_vec()
    } else {
        state
            .iter()
            .cloned()
            .chain(declared.iter().cloned())
            .collect()
    }
}

#[doc(hidden)]
pub fn deserialize<T>(values: Values) -> Result<T, Error>
where
    T: serde::de::DeserializeOwned,
{
    let value = serde_json::Value::Object(values.into_iter().collect());
    serde_json::from_value(value).map_err(Error::Deserialize)
}
