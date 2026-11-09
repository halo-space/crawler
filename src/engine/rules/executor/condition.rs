use super::value::{self, Context};

pub(super) fn matches(when: Option<&str>, context: &Context<'_>) -> Result<bool, crate::Error> {
    let Some(when) = when else {
        return Ok(true);
    };
    if let Some(path) = when.strip_suffix(" != null") {
        return Ok(!value::is_empty(&value::resolve_path(
            path.trim(),
            context,
        )?));
    }
    if let Some(path) = when.strip_suffix(" == null") {
        return Ok(value::is_empty(&value::resolve_path(path.trim(), context)?));
    }
    Err(crate::Error::message(format!(
        "unsupported edge.when expression: {when}"
    )))
}
