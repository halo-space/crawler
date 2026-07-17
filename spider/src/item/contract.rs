use std::any::Any;
use std::collections::HashMap;

use serde_json::Value;

use crate::middleware;

pub trait Item: erased_serde::Serialize + Send + Sync {
    fn from_values(values: crate::item::Values) -> Result<Self, crate::item::Error>
    where
        Self: Sized;

    fn state(&self) -> &crate::item::State;

    fn state_mut(&mut self) -> &mut crate::item::State;

    fn id(&self) -> &str {
        self.state().id()
    }

    fn id_mut(&mut self) -> &mut String {
        self.state_mut().id_mut()
    }

    fn schema(&self) -> Option<crate::item::SchemaKey> {
        self.state().schema()
    }

    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> &mut dyn Any;

    fn vals(&self) -> &HashMap<String, Value> {
        self.state().vals()
    }

    fn vals_mut(&mut self) -> &mut HashMap<String, Value> {
        self.state_mut().vals_mut()
    }

    fn middlewares(&self) -> &[middleware::Spec] {
        &[]
    }
}

erased_serde::serialize_trait_object!(Item);
