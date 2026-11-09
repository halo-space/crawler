use std::future::Future;

use crate::net;

pub trait Download: Send + Sync {
    fn open(&self) -> impl Future<Output = Result<(), crate::downloader::Error>> + Send;

    fn close(&self) -> impl Future<Output = Result<(), crate::downloader::Error>> + Send;

    fn fetch(
        &self,
        request: net::Request,
    ) -> impl Future<Output = Result<net::Response, crate::downloader::Error>> + Send;
}
