use std::sync::{Arc, OnceLock};

use kameo::actor::ReplyRecipient;
use kameo::error::SendError;
use tokio::sync::Notify;

use super::Context;
use super::activity::Activity;
use super::capacity::{Capacity, Permit};
use crate::{item, net};

pub(crate) type Recipient = ReplyRecipient<Event, (), String>;

#[derive(Clone)]
pub(crate) struct Events {
    target: Arc<Target>,
    activity: Activity,
    capacity: Capacity,
}

impl Events {
    pub(super) fn new(activity: Activity, capacity: Capacity) -> Self {
        Self {
            target: Arc::new(Target::default()),
            activity,
            capacity,
        }
    }

    pub(crate) fn bind(&self, recipient: Recipient) -> Result<(), crate::Error> {
        self.target
            .bind(recipient)
            .map_err(|()| crate::Error::message("engine event recipient is already bound"))
    }

    pub(crate) fn set_limit(&self, limit: usize) {
        self.capacity.set(limit);
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.activity.is_idle() && self.capacity.is_idle()
    }

    pub(crate) async fn wait_until_idle(&self) {
        tokio::join!(self.activity.wait(), self.capacity.wait());
    }

    pub(super) async fn send(&self, event: Event) -> Result<(), SendError<Event, String>> {
        self.target.wait().await.ask(event).await
    }
}

#[derive(Default)]
struct Target {
    recipient: OnceLock<Recipient>,
    bound: Notify,
}

impl Target {
    fn bind(&self, recipient: Recipient) -> Result<(), ()> {
        self.recipient.set(recipient).map_err(|_| ())?;
        self.bound.notify_waiters();
        Ok(())
    }

    async fn wait(&self) -> Recipient {
        loop {
            let bound = self.bound.notified();
            if let Some(recipient) = self.recipient.get() {
                return recipient.clone();
            }
            bound.await;
        }
    }
}

pub(crate) struct Event {
    kind: Kind,
    permit: Permit,
}

impl Event {
    pub(super) fn new(kind: Kind, permit: Permit) -> Self {
        Self { kind, permit }
    }

    pub(crate) fn accept(self) -> Kind {
        let Self { kind, permit } = self;
        drop(permit);
        kind
    }
}

pub(crate) enum Kind {
    Requests {
        requests: Vec<net::Request>,
        context: Option<Context>,
    },
    Items {
        items: Vec<Box<dyn item::Item>>,
        context: Option<Context>,
    },
}
