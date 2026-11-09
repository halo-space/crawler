use std::ops::AddAssign;

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Counter {
    pub total: i64,
    pub done: i64,
    pub filter: i64,
    pub dedup: i64,
    pub validate: i64,
    pub download: i64,
}

impl Counter {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    pub(crate) fn is_non_negative(&self) -> bool {
        self.total >= 0
            && self.done >= 0
            && self.filter >= 0
            && self.dedup >= 0
            && self.validate >= 0
            && self.download >= 0
    }

    pub(crate) fn checked_add(&self, other: &Self) -> Option<Self> {
        Some(Self {
            total: self.total.checked_add(other.total)?,
            done: self.done.checked_add(other.done)?,
            filter: self.filter.checked_add(other.filter)?,
            dedup: self.dedup.checked_add(other.dedup)?,
            validate: self.validate.checked_add(other.validate)?,
            download: self.download.checked_add(other.download)?,
        })
    }
}

impl AddAssign<&Self> for Counter {
    fn add_assign(&mut self, other: &Self) {
        self.total += other.total;
        self.done += other.done;
        self.filter += other.filter;
        self.dedup += other.dedup;
        self.validate += other.validate;
        self.download += other.download;
    }
}
