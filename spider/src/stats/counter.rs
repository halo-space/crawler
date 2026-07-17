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
