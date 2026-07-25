use spider::{payload, scheduler};

use super::{Api, state::Action, wire};

impl Api {
    pub(super) async fn submit(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.require_open()?;
        payload
            .validate_items()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        if payload.items.is_empty() {
            return Ok(());
        }

        let items = payload
            .items
            .iter()
            .map(|item| {
                serde_json::to_value(item.as_ref())
                    .map(|data| wire::Record {
                        id: item.id().to_string(),
                        data,
                    })
                    .map_err(|error| scheduler::Error::Message(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let body = wire::Items {
            context: wire::Context::from_payload(payload),
            items,
        };
        self.client.validate_body(&body)?;
        let digest = wire::canonical_digest(&body)
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;
        let (operation, key) = self.operation_key(Action::Items(digest)).await?;
        let result = self
            .client
            .post_empty("v1/worker/items", &body, Some(&key))
            .await;
        self.resolve(operation, result).await
    }
}
