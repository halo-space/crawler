use crate::{net, selector};

pub fn select(response: &net::Response, expr: &str) -> Result<Vec<String>, selector::Error> {
    let text = response
        .text()
        .map_err(|error| selector::Error::Message(error.to_string()))?;
    let regex =
        regex::Regex::new(expr).map_err(|error| selector::Error::Message(error.to_string()))?;
    Ok(regex
        .captures_iter(&text)
        .filter_map(|captures| {
            captures
                .get(1)
                .or_else(|| captures.get(0))
                .map(|value| value.as_str().to_string())
        })
        .collect())
}
