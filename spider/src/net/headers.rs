use std::collections::HashMap;

pub type Headers = HashMap<String, String>;

pub(crate) fn insert(headers: &mut Headers, name: String, value: String) {
    headers.retain(|current, _| !current.eq_ignore_ascii_case(&name));
    headers.insert(name, value);
}
