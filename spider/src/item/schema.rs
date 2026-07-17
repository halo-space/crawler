use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SchemaKey([u8; 32]);

impl SchemaKey {
    fn new(schema: &Value) -> Result<Self, crate::item::Error> {
        let schema = canonical(schema);
        let bytes = serde_json::to_vec(&schema).map_err(crate::item::Error::from)?;
        Ok(Self(Sha256::digest(bytes).into()))
    }

    pub fn as_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

pub enum Output {
    Valid,
    Invalid(Vec<validator::FieldError>),
}

#[derive(Default)]
pub struct Store {
    validators: RwLock<HashMap<SchemaKey, Arc<validator::Validator>>>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, schema: &Value) -> Result<SchemaKey, crate::item::Error> {
        let key = SchemaKey::new(schema)?;
        if self.read().contains_key(&key) {
            return Ok(key);
        }
        let schema = validator::Schema::from_json(schema.to_string())
            .map_err(|error| crate::item::Error::Message(error.to_string()))?;
        let validator = Arc::new(validator::Validator::with_schema(schema));
        if let Err(error) = validator.validate_map(&serde_json::json!({}))
            && !error.is_failed()
        {
            return Err(crate::item::Error::Message(error.to_string()));
        }
        self.write().entry(key).or_insert(validator);
        Ok(key)
    }

    pub fn validate(&self, key: SchemaKey, value: &Value) -> Result<Output, crate::item::Error> {
        let validator = self.read().get(&key).cloned().ok_or_else(|| {
            crate::item::Error::Message(format!("item schema is not registered: {}", key.as_hex()))
        })?;
        match validator.validate_map(value) {
            Ok(()) => Ok(Output::Valid),
            Err(error) if error.is_failed() => {
                Ok(Output::Invalid(error.into_fields().unwrap_or_default()))
            }
            Err(error) => Err(crate::item::Error::Message(error.to_string())),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, HashMap<SchemaKey, Arc<validator::Validator>>> {
        self.validators
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<SchemaKey, Arc<validator::Validator>>> {
        self.validators
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), canonical(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
        value => value.clone(),
    }
}
