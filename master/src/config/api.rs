use crate::Error;

const DEFAULT_SIZE: usize = 64 * 1024 * 1024;
const MIN_SIZE: usize = 1024;
const MAX_SIZE: usize = u32::MAX as usize;

#[derive(Clone, Debug)]
pub struct Api {
    pub max_size: usize,
}

impl Default for Api {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_SIZE,
        }
    }
}

impl Api {
    pub fn validate(&self) -> Result<(), Error> {
        if !(MIN_SIZE..=MAX_SIZE).contains(&self.max_size) {
            return Err(Error::Config(format!(
                "API max size must be between {MIN_SIZE} and {MAX_SIZE} bytes"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_sixty_four_mebibytes() {
        let api = Api::default();

        assert_eq!(api.max_size, DEFAULT_SIZE);
        api.validate().unwrap();
    }

    #[test]
    fn rejects_sizes_outside_the_transport_range() {
        assert!(
            Api {
                max_size: MIN_SIZE - 1,
            }
            .validate()
            .is_err()
        );
        if let Some(too_large) = MAX_SIZE.checked_add(1) {
            assert!(
                Api {
                    max_size: too_large,
                }
                .validate()
                .is_err()
            );
        }
    }
}
