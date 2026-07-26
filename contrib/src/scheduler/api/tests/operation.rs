use super::*;

#[tokio::test]
async fn unresolved_init_keys_survive_only_transient_results() {
    tokio::spawn(async {
        let api = Api::new("https://master.example.com", "token").unwrap();
        let action = Action::Init("init".to_string());
        let (operation, first) = api.operation_key(action.clone()).await.unwrap();
        let transient = api
            .resolve::<()>(
                operation.clone(),
                Err(scheduler::Error::Unavailable("offline".to_string())),
            )
            .await;
        assert!(transient.is_err());
        let (_, retry) = api.operation_key(action.clone()).await.unwrap();
        assert_eq!(retry, first);

        let deterministic = api
            .resolve::<()>(
                operation,
                Err(scheduler::Error::Message("invalid".to_string())),
            )
            .await;
        assert!(deterministic.is_err());
        let (_, later) = api.operation_key(action).await.unwrap();
        assert_ne!(later, first);
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn unresolved_init_keys_expire() {
    tokio::spawn(async {
        let mut operations = Operations::new(Duration::from_millis(10), 1);
        let operation = Operation::new(Action::Init("init".to_string()));
        let first = operations.key(operation.clone()).unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;

        let second = operations.key(operation).unwrap();
        assert_ne!(second, first);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unresolved_operation_capacity_never_evicts_a_live_key() {
    tokio::spawn(async {
        let mut operations = Operations::new(Duration::from_secs(60), 1);
        let first = Operation::new(Action::Init("first".to_string()));
        let second = Operation::new(Action::Init("second".to_string()));
        let key = operations.key(first.clone()).unwrap();

        assert!(matches!(
            operations.key(second.clone()),
            Err(scheduler::Error::Unavailable(_))
        ));
        assert_eq!(operations.key(first.clone()).unwrap(), key);

        operations.remove(&first);
        assert!(operations.key(second).is_ok());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unresolved_operation_ttl_is_fixed_from_key_creation() {
    tokio::spawn(async {
        let mut operations = Operations::new(Duration::from_millis(100), 1);
        let operation = Operation::new(Action::Init("init".to_string()));
        let first = operations.key(operation.clone()).unwrap();

        tokio::time::sleep(Duration::from_millis(70)).await;
        assert_eq!(operations.key(operation.clone()).unwrap(), first);
        tokio::time::sleep(Duration::from_millis(70)).await;

        assert_ne!(operations.key(operation).unwrap(), first);
    })
    .await
    .unwrap();
}

#[test]
fn operations_without_a_tokio_task_are_not_cached() {
    let mut operations = Operations::new(Duration::from_secs(60), 1);
    let operation = Operation::new(Action::Init("init".to_string()));

    assert_ne!(
        operations.key(operation.clone()).unwrap(),
        operations.key(operation).unwrap()
    );
}

#[test]
fn unresolved_operation_limits_are_stable() {
    assert_eq!(OPERATION_TTL, Duration::from_secs(5 * 60));
    assert_eq!(OPERATION_CAPACITY, 4096);
}

#[tokio::test]
async fn independent_init_tasks_do_not_share_an_operation_key() {
    let api = Arc::new(Api::new("https://master.example.com", "token").unwrap());
    let first = {
        let api = api.clone();
        tokio::spawn(async move {
            api.operation_key(Action::Init("same-payload".to_string()))
                .await
                .unwrap()
                .1
        })
    };
    let second = {
        let api = api.clone();
        tokio::spawn(async move {
            api.operation_key(Action::Init("same-payload".to_string()))
                .await
                .unwrap()
                .1
        })
    };
    assert_ne!(first.await.unwrap(), second.await.unwrap());
}

#[test]
fn invocation_keys_are_always_fresh() {
    assert_ne!(Api::invocation_key(), Api::invocation_key());
}
