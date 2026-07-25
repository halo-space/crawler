#[tokio::main]
async fn main() -> Result<(), master::Error> {
    master::Server::from_env().await?.serve().await
}
