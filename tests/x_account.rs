use halo::harbor::config::Config;
use halo::harbor::item::{Jsonl, Map};
use halo::harbor::middleware::{BoxFuture, Middleware, Next, Spec};
use halo::harbor::net::{Request, Response};
use halo::harbor::{Error, Memory, Spider, Tx, engine};

const RULES: &str = r#"
spider:
  name: x_account_posts
  allowed_domains: [api.x.com]
  start:
    - node: user
      url: "https://api.x.com/2/users/by/username/{username}"
      method: GET
      download_mode: http
      middlewares:
        - name: x_auth
          hook: before_download

graph:
  nodes:
    user:
      fields:
        user_id:
          default: ""
          extractors:
            - kind: json
              expr: "$.data.id"
      binds:
        posts_url:
          kind: template
          template: "https://api.x.com/2/users/{user_id}/tweets?max_results=100&tweet.fields=created_at,author_id"
          vars:
            user_id: {from: "$fields.user_id"}
    posts:
      fields:
        posts:
          default: []
          extractors:
            - kind: json
              expr: "$.data[*]"
        next_token:
          default: ""
          extractors:
            - kind: json
              expr: "$.meta.next_token"
      binds:
        next_url:
          kind: template
          template: "https://api.x.com/2/users/{user_id}/tweets?max_results=100&tweet.fields=created_at,author_id&pagination_token={next_token}"
          vars:
            user_id: {from: "$vals.user_id"}
            next_token: {from: "$fields.next_token"}
  edges:
    - from: user
      kind: request
      request:
        node: posts
        url: {from: "$binds.posts_url"}
        method: GET
        download_mode: http
        vals:
          user_id: {from: "$fields.user_id"}
    - from: posts
      kind: request
      when:
        from: "$fields.next_token"
        op: present
      request:
        node: posts
        url: {from: "$binds.next_url"}
        method: GET
        download_mode: http
        vals:
          user_id: {from: "$vals.user_id"}
    - from: posts
      kind: item
      each: "$fields.posts"
      vals:
        source: {value: "x"}
        user_id: {from: "$vals.user_id"}
        post_id: {from: "$item.id"}
        text: {from: "$item.text"}
        created_at: {from: "$item.created_at"}

item:
  schema:
    fields:
      source: {type: string}
      user_id: {type: string}
      post_id: {type: string}
      text: {type: string}
      created_at: {type: string}
"#;

struct XSpider {
    tx: Tx,
}

impl Spider for XSpider {
    type Item = Map;

    fn name(&self) -> &str {
        "x-account-test"
    }
    fn tx(&self) -> &Tx {
        &self.tx
    }

    async fn index(&self, _response: Response) -> Result<(), Error> {
        Ok(())
    }
}

struct XAuth {
    token: String,
}

impl Middleware for XAuth {
    fn before_download<'a>(
        &'a self,
        mut request: Request,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Request>> {
        Box::pin(async move {
            request
                .headers
                .try_set("Authorization", &format!("Bearer {}", self.token))
                .map_err(|error| halo::harbor::middleware::Error::Message(error.to_string()))?;
            Ok(Next::Continue(request))
        })
    }
}

#[tokio::test]
#[ignore = "requires X_USERNAME and X_BEARER_TOKEN"]
async fn x_account_rules_fetch_real_posts_and_follow_next_token() {
    let username =
        std::env::var("X_USERNAME").expect("X_USERNAME must identify the account to crawl");
    let token =
        std::env::var("X_BEARER_TOKEN").expect("X_BEARER_TOKEN must authorize the X API request");
    let config = Config::from_yaml(&RULES.replace("{username}", &username)).unwrap();
    let output_dir = std::env::temp_dir().join(format!("harbor-x-account-{}", std::process::id()));

    let mut engine = engine::Engine::new()
        .with_scheduler(Memory::new())
        .with_middleware("x_auth", XAuth { token })
        .with_rules(config)
        .with_spider(|tx| XSpider { tx })
        .with_store(Jsonl::with_dir(&output_dir))
        .build();

    engine.start_until_idle().await.unwrap();
    assert_eq!(engine.scheduler().failed_len(), 0);
    let trace_id = engine.scheduler().trace_ids().into_iter().next().unwrap();
    let stats = engine.scheduler().trace_stats(&trace_id);
    assert_eq!(stats["user"].done, 1);
    assert!(stats["posts"].done >= 1);
    assert!(stats["items"].done >= 1);
    tokio::fs::remove_dir_all(output_dir).await.unwrap();
}
