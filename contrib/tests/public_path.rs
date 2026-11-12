use halo::contrib::store::redis::Redis;

#[test]
fn redis_item_store_uses_the_public_halo_path() {
    let store = Redis::new("redis://127.0.0.1/", "items").unwrap();
    assert_eq!(store.key(), "items");
}
