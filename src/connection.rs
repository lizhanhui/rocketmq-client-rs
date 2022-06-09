//!
//! This module defines connection related structs.
//!
//!

use crate::error::{self, ClientError};
use crate::frame::{self, Frame};
use bytes::{self, Buf, BytesMut};
use std::collections::HashMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    /// Establish a connection to the given socket address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rocketmq_client::connection::Connection;
    /// use std::net::SocketAddr;
    ///
    /// #[tokio::main]
    /// fn main() {
    ///    let endpoint = "127.0.0.1:80";
    ///    let socket_addr = endpoint.parse::<std::net::SocketAddr>().unwrap();
    ///    let connection = rocketmq_client::connection::Connection::new(&socket_addr).await.unwrap();
    /// }
    ///
    /// ```
    ///
    /// # Errors
    /// Raise ClientError::ConnectTimeout if connection may not be established within reasonable amount of time.
    pub async fn new(endpoint: &SocketAddr) -> Result<Self, error::ClientError> {
        let tcp_stream = TcpStream::connect(endpoint)
            .await
            .map_err(|e| error::ClientError::ConnectTimeout(e))?;

        Ok(Connection {
            stream: BufWriter::new(tcp_stream),
            buffer: BytesMut::with_capacity(1024 * 1024),
        })
    }

    pub async fn read_frame(&mut self) -> Result<Option<frame::Frame>, ClientError> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(ClientError::ConnectionReset);
                }
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), ClientError> {
        if let Some(buf) = frame.encode()? {
            self.stream.write_all(&buf.slice(..)).await?;
            self.stream.flush().await?;
        }
        Ok(())
    }

    fn parse_frame(&mut self) -> Result<Option<frame::Frame>, ClientError> {
        let mut buf = Cursor::new(&self.buffer[..]);
        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;
                buf.set_position(0);
                let frame = Frame::parse(&mut buf)?;
                self.buffer.advance(len);
                return Ok(frame);
            }

            Err(frame::Error::Incomplete) => {
                return Ok(None);
            }

            Err(frame::Error::Other(e)) => {
                return Err(e);
            }
        }
    }
}

pub(crate) struct ConnectionManager {
    connections: Arc<Mutex<HashMap<String, Connection>>>,
}

impl ConnectionManager {
    pub(crate) fn new() -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::TopicRouteData;

    use super::*;
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_connection_new() -> Result<(), error::ClientError> {
        let addr = "127.0.0.1:9876";
        let endpoint: SocketAddr = addr
            .parse()
            .map_err(|_e| error::ClientError::BadAddress(addr.to_string()))?;
        let _connection = Connection::new(&endpoint).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_read_write_frame() -> Result<(), ClientError> {
        let mut frame = Frame::new();
        frame.code = frame::RequestCode::GetRouteInfoByTopic as i32;
        frame.language = crate::frame::Language::CPP;
        frame.put_ext_field("topic", "T1");
        let addr = "127.0.0.1:9876";
        let endpoint: SocketAddr = addr
            .parse()
            .map_err(|_e| error::ClientError::BadAddress(addr.to_string()))?;
        let mut connection = Connection::new(&endpoint).await?;
        connection.write_frame(&frame).await?;
        if let Some(response) = connection.read_frame().await? {
            assert_eq!(response.frame_type(), frame::Type::Response);
            if 0 == response.code {
                let body = response.body();
                let topic_route_data: TopicRouteData = serde_json::from_reader(body.reader())
                    .map_err(|_e| {
                        return crate::error::ClientError::InvalidFrame(
                            "Response body is invalid JSON".to_owned(),
                        );
                    })?;
                topic_route_data.broker_datas.iter().for_each(|item| {
                    println!("{:#?}", item);
                });
                topic_route_data.queue_datas.iter().for_each(|item| {
                    println!("{:#?}", item);
                });
            }
            println!("Remark: {}", response.remark());
        }

        Ok(())
    }
}
