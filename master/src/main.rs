#[tokio::main]
async fn main() -> Result<(), master::Error> {
    let mut args = std::env::args_os().skip(1);
    let path = match args.next().as_deref() {
        Some(value) if value.to_str() == Some("--config") => args
            .next()
            .ok_or_else(|| master::Error::Config("--config requires a file path".to_string()))?,
        Some(value) => {
            return Err(master::Error::Config(format!(
                "unknown argument: {}",
                value.to_string_lossy()
            )));
        }
        None => {
            return Err(master::Error::Config(
                "usage: master --config <path>".to_string(),
            ));
        }
    };
    if let Some(value) = args.next() {
        return Err(master::Error::Config(format!(
            "unexpected argument: {}",
            value.to_string_lossy()
        )));
    }
    let config = master::Config::from_file(path)?;
    master::Server::from_config(config).await?.serve().await
}
