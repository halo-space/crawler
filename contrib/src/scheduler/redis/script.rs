pub(super) struct Scripts {
    pub(super) init: redis::Script,
    pub(super) push: redis::Script,
    pub(super) claim: redis::Script,
    pub(super) recover: redis::Script,
    pub(super) pending: redis::Script,
    pub(super) ack: redis::Script,
    pub(super) release: redis::Script,
    pub(super) refresh: redis::Script,
    pub(super) success: redis::Script,
    pub(super) failure: redis::Script,
}

impl Scripts {
    pub(super) fn new() -> Self {
        Self {
            init: redis::Script::new(include_str!("scripts/init.lua")),
            push: redis::Script::new(include_str!("scripts/push.lua")),
            claim: redis::Script::new(include_str!("scripts/claim.lua")),
            recover: redis::Script::new(include_str!("scripts/recover.lua")),
            pending: redis::Script::new(include_str!("scripts/pending.lua")),
            ack: redis::Script::new(include_str!("scripts/ack.lua")),
            release: redis::Script::new(include_str!("scripts/release.lua")),
            refresh: redis::Script::new(include_str!("scripts/refresh.lua")),
            success: redis::Script::new(include_str!("scripts/success.lua")),
            failure: redis::Script::new(include_str!("scripts/failure.lua")),
        }
    }

    pub(super) async fn load(
        &self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<()> {
        for script in [
            &self.init,
            &self.push,
            &self.claim,
            &self.recover,
            &self.pending,
            &self.ack,
            &self.release,
            &self.refresh,
            &self.success,
            &self.failure,
        ] {
            script.load_async(connection).await?;
        }
        Ok(())
    }
}
