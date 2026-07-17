use crate::{net, spider};

pub type Call<S> = for<'a> fn(&'a S, <S as spider::Spider>::Item) -> net::BoxFuture<'a>;

pub struct Function<S>
where
    S: spider::Spider,
{
    name: &'static str,
    call: Call<S>,
}

impl<S> Function<S>
where
    S: spider::Spider,
{
    pub fn new(name: &'static str, call: Call<S>) -> Self {
        Self { name, call }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn call<'a>(&self, spider: &'a S, item: S::Item) -> net::BoxFuture<'a> {
        (self.call)(spider, item)
    }
}
