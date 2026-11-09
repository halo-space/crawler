use serde_json::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    #[serde(default)]
    pub hook: Option<String>,
    pub name: String,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub order: Option<i32>,
    #[serde(default)]
    pub skip: bool,
    #[serde(default)]
    pub args: Value,
}

impl Spec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    pub fn hook(mut self, hook: impl Into<String>) -> Self {
        self.hook = Some(hook.into());
        self
    }

    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn order(mut self, order: i32) -> Self {
        self.order = Some(order);
        self
    }

    pub fn args(mut self, args: Value) -> Self {
        self.args = args;
        self
    }
}
