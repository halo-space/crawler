use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;

use crate::downloader;

pub(super) async fn read(
    response: reqwest::Response,
    limit: u64,
) -> Result<Bytes, downloader::Error> {
    if limit == 0 {
        return Err(downloader::Error::InvalidConfig(
            "max_body_bytes must be positive".to_string(),
        ));
    }
    if response
        .content_length()
        .is_some_and(|content_length| content_length > limit)
    {
        return Err(downloader::Error::BodyTooLarge { limit });
    }

    let mut stream = response.bytes_stream();
    let mut body = BytesMut::new();
    let mut size = 0_u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let chunk_size =
            u64::try_from(chunk.len()).map_err(|_| downloader::Error::BodyTooLarge { limit })?;
        if chunk_size > limit - size {
            return Err(downloader::Error::BodyTooLarge { limit });
        }
        size += chunk_size;
        body.extend_from_slice(&chunk);
    }

    Ok(body.freeze())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    use super::*;

    fn read_request(stream: &mut TcpStream) {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 256];
        while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            let size = stream.read(&mut chunk).unwrap();
            if size == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..size]);
        }
    }

    fn serve(response: Vec<u8>) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream.write_all(&response).unwrap();
        });
        (url, server)
    }

    async fn response(raw: impl Into<Vec<u8>>) -> (reqwest::Response, thread::JoinHandle<()>) {
        let (url, server) = serve(raw.into());
        let response = reqwest::Client::new().get(url).send().await.unwrap();
        (response, server)
    }

    #[tokio::test]
    async fn accepts_a_body_exactly_at_the_limit() {
        let (response, server) = response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\n12345678".to_vec(),
        )
        .await;

        let body = read(response, 8).await.unwrap();
        server.join().unwrap();

        assert_eq!(body.as_ref(), b"12345678");
    }

    #[tokio::test]
    async fn rejects_a_chunked_body_on_the_first_over_limit_chunk() {
        let (response, server) = response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\n1234\r\n4\r\n5678\r\n0\r\n\r\n"
                .to_vec(),
        )
        .await;

        let error = read(response, 7).await.unwrap_err();
        server.join().unwrap();

        assert!(matches!(
            error,
            downloader::Error::BodyTooLarge { limit: 7 }
        ));
    }

    #[tokio::test]
    async fn rejects_a_declared_oversized_body_without_reading_it() {
        let (response, server) =
            response(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\n".to_vec())
                .await;

        let error = read(response, 8).await.unwrap_err();
        server.join().unwrap();

        assert!(matches!(
            error,
            downloader::Error::BodyTooLarge { limit: 8 }
        ));
    }

    #[tokio::test]
    async fn enforces_the_limit_after_content_decoding() {
        const GZIP_BODY: &[u8] = &[
            31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 75, 76, 196, 15, 0, 119, 23, 177, 202, 32, 0, 0, 0,
        ];
        let mut raw = format!(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            GZIP_BODY.len()
        )
        .into_bytes();
        raw.extend_from_slice(GZIP_BODY);
        let (response, server) = response(raw).await;

        let error = read(response, 24).await.unwrap_err();
        server.join().unwrap();

        assert!(matches!(
            error,
            downloader::Error::BodyTooLarge { limit: 24 }
        ));
    }
}
