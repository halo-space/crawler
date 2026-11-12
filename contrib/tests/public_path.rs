use halo::contrib::store::redis::Redis;

#[test]
fn redis_item_store_uses_the_public_halo_path() {
    let store = Redis::with_namespace("redis://127.0.0.1/", "harbor")
        .unwrap()
        .with_topic("items")
        .unwrap();
    assert_eq!(store.topic_key(), "harbor:items");
}
