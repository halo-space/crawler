#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), harbor_worker::Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("rustls crypto provider must be installed once");
    let config = harbor_worker::Config::from_env()?;
    harbor_worker::run(config).await
}
