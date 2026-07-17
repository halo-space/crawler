use std::sync::{Arc, OnceLock};

use tokio::sync::{mpsc, oneshot};

use crate::{Error, error::spider::Error as SpiderError, item, net};

mod activity;
mod capacity;
mod context;
mod event;

use activity::{Activity, Registration};
use capacity::Capacity;
pub(crate) use context::{Context, scope};
pub(crate) use event::{Event, Receiver, Reply};

pub struct Tx {
    sender: mpsc::Sender<Event>,
    activity: Activity,
    capacity: Capacity,
    trace: Arc<OnceLock<Trace>>,
    detached: Option<Trace>,
    _registration: Option<Registration>,
}

#[derive(Clone)]
struct Trace {
    task_id: String,
    trace_id: String,
}

pub(crate) fn channel(buffer: usize) -> (Tx, Receiver) {
    let (sender, receiver) = mpsc::channel(buffer);
    let activity = Activity::default();
    let capacity = Capacity::new(buffer);
    (
        Tx::new(
            sender,
            activity.clone(),
            capacity.clone(),
            Arc::new(OnceLock::new()),
            None,
        ),
        Receiver::new(receiver, activity, capacity),
    )
}

impl Clone for Tx {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            activity: self.activity.clone(),
            capacity: self.capacity.clone(),
            trace: self.trace.clone(),
            detached: context::current()
                .map(|context| Trace::new(context.task_id(), context.trace_id()))
                .or_else(|| self.detached.clone()),
            _registration: Some(Registration::new(self.activity.clone())),
        }
    }
}

impl Tx {
    fn new(
        sender: mpsc::Sender<Event>,
        activity: Activity,
        capacity: Capacity,
        trace: Arc<OnceLock<Trace>>,
        detached: Option<Trace>,
    ) -> Self {
        Self {
            sender,
            activity,
            capacity,
            trace,
            detached,
            _registration: None,
        }
    }

    fn context(&self) -> Option<Context> {
        context::current().or_else(|| {
            self.detached
                .as_ref()
                .or_else(|| self.trace.as_ref().get())
                .map(Trace::context)
        })
    }

    pub(crate) fn set_trace(&self, task_id: impl Into<String>, trace_id: impl Into<String>) {
        assert!(
            self.trace.set(Trace::new(task_id, trace_id)).is_ok(),
            "Tx Trace can only be initialized once"
        );
    }

    pub async fn request(&self, mut requests: Vec<net::Request>) -> Result<(), Error> {
        let context = self.context();
        if let Some(context) = &context {
            for request in &mut requests {
                if request.task_id.is_empty() {
                    request.task_id = context.task_id().to_string();
                }
                if request.trace_id.is_empty() {
                    request.trace_id = context.trace_id().to_string();
                }
            }
        }

        let (reply, completion) = oneshot::channel();
        let permit = self.capacity.acquire().await;
        self.sender
            .send(Event::Requests {
                requests,
                context,
                reply,
                permit,
            })
            .await
            .map_err(|_| Error::Spider(SpiderError::ChannelClosed))?;

        completion
            .await
            .map_err(|_| Error::Spider(SpiderError::ChannelClosed))?
            .map_err(|error| Error::Spider(SpiderError::RequestRejected(error)))
    }

    pub async fn item<I>(&self, items: Vec<I>) -> Result<(), Error>
    where
        I: item::Item + 'static,
    {
        let items = items
            .into_iter()
            .map(|mut item| {
                if item.id().is_empty() {
                    *item.id_mut() = uuid::Uuid::now_v7().to_string();
                }
                Box::new(item) as Box<dyn item::Item>
            })
            .collect();

        let (reply, completion) = oneshot::channel();
        let permit = self.capacity.acquire().await;
        self.sender
            .send(Event::Items {
                items,
                context: self.context(),
                reply,
                permit,
            })
            .await
            .map_err(|_| Error::Spider(SpiderError::ChannelClosed))?;

        completion
            .await
            .map_err(|_| Error::Spider(SpiderError::ChannelClosed))?
            .map_err(|error| Error::Spider(SpiderError::ItemRejected(error)))
    }
}

impl Trace {
    fn new(task_id: impl Into<String>, trace_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            trace_id: trace_id.into(),
        }
    }

    fn context(&self) -> Context {
        Context::trace(self.task_id.clone(), self.trace_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;

    #[derive(serde::Serialize)]
    struct TestItem {
        #[serde(skip)]
        state: item::State,
    }

    impl TestItem {
        fn new(id: impl Into<String>) -> Self {
            Self {
                state: {
                    let mut state = item::State::default();
                    *state.id_mut() = id.into();
                    state
                },
            }
        }
    }

    impl item::Item for TestItem {
        fn from_values(_values: item::Values) -> Result<Self, item::Error> {
            Ok(Self::new(String::new()))
        }

        fn state(&self) -> &item::State {
            &self.state
        }

        fn state_mut(&mut self) -> &mut item::State {
            &mut self.state
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    async fn emitted_item(item: TestItem) -> Box<dyn item::Item> {
        let (tx, mut events) = channel(1);
        let send = tokio::spawn(async move { tx.item(vec![item]).await });
        let Event::Items {
            mut items, reply, ..
        } = events.recv().await.unwrap()
        else {
            panic!("expected item event");
        };
        let _ = reply.send(Ok(()));
        send.await.unwrap().unwrap();
        items.pop().unwrap()
    }

    async fn complete(event: Event) {
        match event {
            Event::Requests { reply, .. } | Event::Items { reply, .. } => {
                let _ = reply.send(Ok(()));
            }
        }
    }

    #[tokio::test]
    async fn item_generates_uuid_v7_for_empty_id() {
        let item = emitted_item(TestItem::new("")).await;
        let id = uuid::Uuid::parse_str(item.id()).unwrap();

        assert_eq!(id.get_version(), Some(uuid::Version::SortRand));
    }

    #[tokio::test]
    async fn item_preserves_existing_id() {
        let item = emitted_item(TestItem::new("business-item-id")).await;

        assert_eq!(item.id(), "business-item-id");
    }

    #[tokio::test]
    async fn event_limit_blocks_the_next_send_until_handling_finishes() {
        let (tx, mut events) = channel(3);
        events.set_limit(2);
        let first = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.request(vec![net::Request::follow("https://example.com/1").unwrap()])
                    .await
            }
        });
        let second = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.request(vec![net::Request::follow("https://example.com/2").unwrap()])
                    .await
            }
        });
        let first_event = events.recv().await.unwrap();
        let second_event = events.recv().await.unwrap();
        let third = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.request(vec![net::Request::follow("https://example.com/3").unwrap()])
                    .await
            }
        });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), events.recv())
                .await
                .is_err()
        );
        complete(first_event).await;
        let third_event =
            tokio::time::timeout(std::time::Duration::from_millis(100), events.recv())
                .await
                .unwrap()
                .unwrap();
        complete(second_event).await;
        complete(third_event).await;

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        third.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelling_sender_does_not_release_an_accepted_event() {
        let (tx, mut events) = channel(2);
        events.set_limit(1);
        let first = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.request(vec![net::Request::follow("https://example.com/1").unwrap()])
                    .await
            }
        });
        let first_event = events.recv().await.unwrap();
        first.abort();
        let second = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.request(vec![net::Request::follow("https://example.com/2").unwrap()])
                    .await
            }
        });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), events.recv())
                .await
                .is_err()
        );
        drop(first_event);
        let second_event =
            tokio::time::timeout(std::time::Duration::from_millis(100), events.recv())
                .await
                .unwrap()
                .unwrap();
        complete(second_event).await;
        second.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn event_limit_above_the_default_is_not_clamped() {
        let limit = crate::engine::MAX_EVENTS + 1;
        let (tx, mut events) = channel(limit);
        events.set_limit(limit);
        let mut sends = Vec::with_capacity(limit);
        for index in 0..limit {
            let tx = tx.clone();
            sends.push(tokio::spawn(async move {
                tx.request(vec![
                    net::Request::follow(format!("https://example.com/{index}")).unwrap(),
                ])
                .await
            }));
        }

        let mut accepted = Vec::with_capacity(limit);
        for _ in 0..limit {
            accepted.push(
                tokio::time::timeout(std::time::Duration::from_millis(100), events.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        for event in accepted {
            complete(event).await;
        }
        for send in sends {
            send.await.unwrap().unwrap();
        }
    }
}
