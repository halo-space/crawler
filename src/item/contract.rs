use std::any::Any;
use std::collections::HashMap;

use serde_json::Value;

use crate::middleware;

pub trait Item: erased_serde::Serialize + Any + Send + Sync {
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

    fn vals(&self) -> &HashMap<String, Value> {
        self.state().vals()
    }

    fn vals_mut(&mut self) -> &mut HashMap<String, Value> {
        self.state_mut().vals_mut()
    }

    fn values(&self) -> Result<crate::item::Values, crate::item::Error> {
        Err(crate::item::Error::Message(
            "Item does not support value inspection".to_string(),
        ))
    }

    fn replace_values(&mut self, _values: crate::item::Values) -> Result<(), crate::item::Error> {
        Err(crate::item::Error::Message(
            "Item does not support value replacement".to_string(),
        ))
    }

    fn middlewares(&self) -> &[middleware::Spec] {
        self.state().middlewares()
    }

    fn middlewares_mut(&mut self) -> &mut Vec<middleware::Spec> {
        self.state_mut().middlewares_mut()
    }
}

impl dyn Item {
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Any,
    {
        (self as &dyn Any).downcast_ref()
    }

    pub fn downcast_mut<T>(&mut self) -> Option<&mut T>
    where
        T: Any,
    {
        (self as &mut dyn Any).downcast_mut()
    }
}

erased_serde::serialize_trait_object!(Item);
