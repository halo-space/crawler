use std::sync::Arc;

use harbor::load::{Error, Load, Message};
use tokio::sync::{Mutex, mpsc};

pub mod redis;

/// An in-memory task configuration queue.
#[derive(Clone)]
pub struct Memory {
    sender: mpsc::Sender<Message>,
    receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
}

impl Memory {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::Message(
                "Memory Load capacity must be positive".to_string(),
            ));
        }
        let (sender, receiver) = mpsc::channel(capacity);
        Ok(Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
        })
    }

    pub async fn push(&self, message: Message) -> Result<(), Error> {
        message.validate()?;
        self.sender
            .send(message)
            .await
            .map_err(|_| Error::Message("Memory Load queue is closed".to_string()))
    }
}

impl Load for Memory {
    fn load<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Message>, Error>> + Send + 'a>,
    > {
        Box::pin(async move { Ok(self.receiver.lock().await.recv().await) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Message {
        Message::new("task-1", "trace-1", harbor::trace::Snapshot::code("task-1"))
    }

    #[tokio::test]
    async fn push_and_load_transfer_one_message() {
        let load = Memory::new(1).unwrap();
        load.push(message()).await.unwrap();

        let loaded = load.load().await.unwrap().unwrap();
        assert_eq!(loaded.task_id, "task-1");
        assert_eq!(loaded.trace_id, "trace-1");
    }

    #[test]
    fn zero_capacity_is_rejected() {
        assert!(Memory::new(0).is_err());
    }
}
