#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Part<'a> {
    Text(&'a str),
    Variable(&'a str),
}

pub(crate) fn parse(template: &str) -> Result<Vec<Part<'_>>, String> {
    let mut parts = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = template[cursor..].find(['{', '}']) {
        let open = cursor + offset;
        if template.as_bytes()[open] == b'}' {
            return Err("closing brace has no matching opening brace".to_string());
        }
        if open > cursor {
            parts.push(Part::Text(&template[cursor..open]));
        }

        let after_open = &template[open + 1..];
        let close = after_open
            .find('}')
            .ok_or_else(|| "opening brace has no matching closing brace".to_string())?;
        let name = &after_open[..close];
        if name.is_empty()
            || name.contains('{')
            || !name
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '.'))
        {
            return Err(format!("invalid variable name: {name}"));
        }
        parts.push(Part::Variable(name));
        cursor = open + close + 2;
    }
    if cursor < template.len() {
        parts.push(Part::Text(&template[cursor..]));
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_text_and_variables_in_source_order() {
        assert_eq!(
            parse("prefix-{item.name}-{idx}").unwrap(),
            [
                Part::Text("prefix-"),
                Part::Variable("item.name"),
                Part::Text("-"),
                Part::Variable("idx")
            ]
        );
    }

    #[test]
    fn rejects_unbalanced_braces_and_invalid_names() {
        for template in ["}", "{", "{}", "{a-b}", "{{name}}"] {
            assert!(parse(template).is_err(), "{template}");
        }
    }
}
