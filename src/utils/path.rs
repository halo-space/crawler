use sha2::{Digest, Sha256};

const MAX_PLAIN_BYTES: usize = 64;
const PREFIX_BYTES: usize = 32;

pub(crate) fn segment(value: &str) -> String {
    if portable(value) {
        return value.to_string();
    }

    let mut prefix = String::with_capacity(PREFIX_BYTES);
    let mut separator = false;
    for character in value.chars() {
        let character = if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
            separator = false;
            character
        } else if separator {
            continue;
        } else {
            separator = true;
            '_'
        };
        if prefix.len() == PREFIX_BYTES {
            break;
        }
        prefix.push(character);
    }
    let prefix = prefix.trim_matches(['-', '_']).trim_end_matches('.');
    let prefix = if prefix.is_empty() { "id" } else { prefix };
    let digest = Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}--{digest}")
}

fn portable(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PLAIN_BYTES
        && !value.bytes().any(|byte| byte.is_ascii_uppercase())
        && !matches!(value, "." | "..")
        && !value.ends_with('.')
        && !reserved(value)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn reserved(value: &str) -> bool {
    let stem = value.split('.').next().unwrap_or_default();
    let stem = stem.to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || stem
            .strip_prefix("COM")
            .or_else(|| stem.strip_prefix("LPT"))
            .is_some_and(|suffix| {
                matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
            })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_portable_short_values() {
        assert_eq!(segment("task-1.example"), "task-1.example");
    }

    #[test]
    fn unsafe_values_with_the_same_prefix_do_not_collide() {
        let slash = segment("task/a");
        let question = segment("task?a");

        assert!(slash.starts_with("task_a--"));
        assert!(question.starts_with("task_a--"));
        assert_ne!(slash, question);
        assert!(!slash.contains('/'));
    }

    #[test]
    fn identities_that_differ_only_by_case_do_not_share_a_path() {
        let lower = segment("task");
        let upper = segment("Task");

        assert_eq!(lower, "task");
        assert_ne!(lower.to_ascii_lowercase(), upper.to_ascii_lowercase());
    }

    #[test]
    fn empty_special_reserved_and_long_values_are_hashed() {
        for value in ["", ".", "..", "CON", "task.", "task..", &"x".repeat(65)] {
            let segment = segment(value);
            assert!(segment.contains("--"), "value: {value}");
            assert!(segment.len() <= PREFIX_BYTES + 2 + 64);
        }
    }
}
