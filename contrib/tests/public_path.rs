use halo::contrib::cron::Crontab;
use halo::contrib::load::redis::Redis as LoadRedis;
use halo::contrib::store::redis::Redis;

#[test]
fn redis_item_store_uses_the_public_halo_path() {
    let store = Redis::with_namespace("redis://127.0.0.1/", "harbor")
        .unwrap()
        .with_topic("items")
        .unwrap();
    assert_eq!(store.topic_key(), "harbor:items");
}

#[test]
fn redis_load_uses_the_public_halo_path() {
    let load = LoadRedis::new("redis://127.0.0.1/", "task-config").unwrap();
    assert_eq!(load.key(), "task-config");
}

#[test]
fn cron_uses_the_public_halo_path() {
    let cron = Crontab::with_timezone("0 10 * * 1-5", "Asia/Shanghai").unwrap();

    assert_eq!(cron.expression(), "0 10 * * 1-5");
    assert_eq!(cron.timezone().name(), "Asia/Shanghai");
}
