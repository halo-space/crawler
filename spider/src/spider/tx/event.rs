use tokio::sync::{mpsc, oneshot};

use super::Context;
use super::activity::Activity;
use super::capacity::{Capacity, Permit};
use crate::{item, net};

pub(crate) type Reply = oneshot::Sender<Result<(), String>>;

pub(crate) struct Receiver {
    receiver: mpsc::Receiver<Event>,
    activity: Activity,
    capacity: Capacity,
}

impl Receiver {
    pub(super) fn new(
        receiver: mpsc::Receiver<Event>,
        activity: Activity,
        capacity: Capacity,
    ) -> Self {
        Self {
            receiver,
            activity,
            capacity,
        }
    }

    pub(crate) fn set_limit(&self, limit: usize) {
        self.capacity.set(limit);
    }

    pub(crate) async fn recv(&mut self) -> Option<Event> {
        self.receiver.recv().await
    }

    pub(crate) fn try_recv(&mut self) -> Result<Event, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    pub(crate) fn producers_are_idle(&self) -> bool {
        self.activity.is_idle()
    }

    pub(crate) fn producer_wait(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let activity = self.activity.clone();
        async move { activity.wait().await }
    }
}

pub(crate) enum Event {
    Requests {
        requests: Vec<net::Request>,
        context: Option<Context>,
        reply: Reply,
        permit: Permit,
    },
    Items {
        items: Vec<Box<dyn item::Item>>,
        context: Option<Context>,
        reply: Reply,
        permit: Permit,
    },
}
