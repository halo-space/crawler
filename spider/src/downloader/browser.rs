use crate::{downloader, net};

#[derive(Default)]
pub struct Browser;

impl downloader::Download for Browser {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, _request: net::Request) -> Result<net::Response, downloader::Error> {
        Err(downloader::Error::UnsupportedMode("browser".to_string()))
    }
}
