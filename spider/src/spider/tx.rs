use std::sync::{Arc, OnceLock};

use crate::{Error, error::spider::Error as SpiderError, item, net};

mod activity;
mod capacity;
mod context;
mod event;

use activity::{Activity, Registration};
use capacity::Capacity;
pub(crate) use context::{Context, scope};
pub(crate) use event::{Event, Events, Kind};

pub struct Tx {
    events: Events,
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

pub(crate) fn channel(buffer: usize) -> (Tx, Events) {
    let activity = Activity::default();
    let capacity = Capacity::new(buffer);
    let events = Events::new(activity.clone(), capacity.clone());
    (
        Tx::new(
            events.clone(),
            activity.clone(),
            capacity.clone(),
            Arc::new(OnceLock::new()),
            None,
        ),
        events,
    )
}

impl Clone for Tx {
    fn clone(&self) -> Self {
        Self {
            events: self.events.clone(),
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
        events: Events,
        activity: Activity,
        capacity: Capacity,
        trace: Arc<OnceLock<Trace>>,
        detached: Option<Trace>,
    ) -> Self {
        Self {
            events,
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

        let permit = self.capacity.acquire().await;
        match self
            .events
            .send(Event::new(Kind::Requests { requests, context }, permit))
            .await
        {
            Ok(()) => Ok(()),
            Err(kameo::error::SendError::HandlerError(error)) => {
                Err(Error::Spider(SpiderError::RequestRejected(error)))
            }
            Err(_) => Err(Error::Spider(SpiderError::EngineStopped)),
        }
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

        let permit = self.capacity.acquire().await;
        match self
            .events
            .send(Event::new(
                Kind::Items {
                    items,
                    context: self.context(),
                },
                permit,
            ))
            .await
        {
            Ok(()) => Ok(()),
            Err(kameo::error::SendError::HandlerError(error)) => {
                Err(Error::Spider(SpiderError::ItemRejected(error)))
            }
            Err(_) => Err(Error::Spider(SpiderError::EngineStopped)),
        }
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

    use kameo::actor::{Actor, ActorRef, Spawn};
    use kameo::message::{Context as ActorContext, Message};
    use kameo::reply::{DelegatedReply, ReplySender};
    use tokio::sync::mpsc;

    use super::*;

    struct Sink {
        accepted: mpsc::UnboundedSender<Accepted>,
    }

    struct Accepted {
        kind: Kind,
        reply: Option<ReplySender<Result<(), String>>>,
    }

    impl Actor for Sink {
        type Args = Self;
        type Error = kameo::error::Infallible;

        async fn on_start(
            sink: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(sink)
        }
    }

    impl Message<Event> for Sink {
        type Reply = DelegatedReply<Result<(), String>>;

        async fn handle(
            &mut self,
            event: Event,
            ctx: &mut ActorContext<Self, Self::Reply>,
        ) -> Self::Reply {
            let kind = event.accept();
            let (delegated, reply) = ctx.reply_sender();
            let _ = self.accepted.send(Accepted { kind, reply });
            delegated
        }
    }

    fn test_channel(
        initial_limit: usize,
        limit: usize,
    ) -> (Tx, mpsc::UnboundedReceiver<Accepted>, ActorRef<Sink>) {
        let (tx, events) = channel(initial_limit);
        events.set_limit(limit);
        let (accepted, receiver) = mpsc::unbounded_channel();
        let prepared = Sink::prepare_with_mailbox(kameo::mailbox::unbounded());
        let actor_ref = prepared.actor_ref().clone();
        events
            .bind(actor_ref.clone().reply_recipient::<Event>())
            .unwrap();
        drop(prepared.spawn(Sink { accepted }));
        (tx, receiver, actor_ref)
    }

    fn complete(reply: Option<ReplySender<Result<(), String>>>) {
        if let Some(reply) = reply {
            reply.send(Ok(()));
        }
    }

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
        let (tx, mut accepted, _actor) = test_channel(1, 1);
        let send = tokio::spawn(async move { tx.item(vec![item]).await });
        let Accepted { kind, reply } = accepted.recv().await.unwrap();
        let Kind::Items { mut items, .. } = kind else {
            panic!("expected item event");
        };
        complete(reply);
        send.await.unwrap().unwrap();
        items.pop().unwrap()
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
    async fn event_limit_is_released_when_handling_starts() {
        let (tx, mut accepted, _actor) = test_channel(3, 1);
        let first = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.request(vec![net::Request::follow("https://example.com/1").unwrap()])
                    .await
            }
        });
        let first_event = accepted.recv().await.unwrap();
        assert!(!first.is_finished());

        let second = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.request(vec![net::Request::follow("https://example.com/2").unwrap()])
                    .await
            }
        });
        let second_event =
            tokio::time::timeout(std::time::Duration::from_millis(100), accepted.recv())
                .await
                .unwrap()
                .unwrap();
        assert!(!second.is_finished());

        complete(first_event.reply);
        complete(second_event.reply);

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelling_sender_does_not_cancel_an_accepted_event() {
        let (tx, mut accepted, _actor) = test_channel(2, 1);
        let first = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.request(vec![net::Request::follow("https://example.com/1").unwrap()])
                    .await
            }
        });
        let first_event = accepted.recv().await.unwrap();
        first.abort();
        let second = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.request(vec![net::Request::follow("https://example.com/2").unwrap()])
                    .await
            }
        });

        let second_event =
            tokio::time::timeout(std::time::Duration::from_millis(100), accepted.recv())
                .await
                .unwrap()
                .unwrap();
        complete(first_event.reply);
        complete(second_event.reply);
        second.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn event_limit_above_the_default_is_not_clamped() {
        let limit = crate::engine::MAX_EVENTS + 1;
        let (tx, mut accepted, _actor) = test_channel(1, limit);
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

        let mut events = Vec::with_capacity(limit);
        for _ in 0..limit {
            events.push(
                tokio::time::timeout(std::time::Duration::from_millis(100), accepted.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        for event in events {
            complete(event.reply);
        }
        for send in sends {
            send.await.unwrap().unwrap();
        }
    }
}
