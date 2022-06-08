use bytes::{self, Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::default::Default;
use std::io::Cursor;
use std::sync::atomic::{self, Ordering};

use crate::error::{self, ClientError};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub(crate) enum Language {
    JAVA,
    CPP,
    RUST,
}

impl Default for Language {
    fn default() -> Self {
        Language::RUST
    }
}


pub(crate) enum RequestCode {
    GetRouteInfoByTopic = 105,
    SendMessage = 10,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Frame {
    // Operation code
    pub(crate) code: i32,

    // Language of the SDK that generates this frame
    pub(crate) language: Language,

    // Version of the SDK that generates this frame
    pub(crate) version: i32,

    // frame identifier
    pub(crate) opaque: i32,

    // Bit-wise flag that overrides semantics of certain fields
    pub(crate) flag: i32,

    // Human readable remarks
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) remark: String,

    #[serde(skip_serializing_if = "HashMap::is_empty", default = "HashMap::new")]
    pub(crate) ext_fields: HashMap<String, String>,

    #[serde(skip)]
    pub(crate) body: bytes::Bytes,
}

#[derive(Debug)]
pub(crate) enum Error {
    // Not enough data is available to parse a message
    Incomplete,

    // Invalid message encoding
    Other(error::ClientError),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Type {
    Request,
    Response,
}

impl Frame {
    // Generate next opaque, aka, request identifier.
    fn next_opaque() -> i32 {
        static SEQUENCE: atomic::AtomicI32 = atomic::AtomicI32::new(0);
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn new() -> Self {
        Frame {
            opaque: Frame::next_opaque(),
            ..Default::default()
        }
    }

    pub(crate) fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        // frame-length = 4 + len(header) + len(body)
        // frame-layout |header-length|---header-data---|---body---|
        let frame_length = Frame::read_i32(src)? as usize;

        if src.remaining() < frame_length {
            return Err(Error::Incomplete);
        }

        src.advance(frame_length);

        Ok(())
    }

    pub(crate) fn parse(src: &mut Cursor<&[u8]>) -> Result<Option<Self>, ClientError> {
        let frame_length = Frame::read_i32(src).map_err(|_e| {
            return ClientError::InvalidFrame("Invalid frame length".to_string());
        })?;
        let header_length = Frame::read_i32(src).map_err(|_e| {
            return ClientError::InvalidFrame("Invalid frame header length".to_string());
        })?;

        let header = src.copy_to_bytes(header_length as usize);
        let mut frame: Frame = serde_json::from_reader(header.reader()).map_err(|_e| {
            return ClientError::InvalidFrame("Invalid frame header JSON".to_string());
        })?;

        let body_length = frame_length - 4 - header_length;
        if body_length > 0 {
            let body = src.copy_to_bytes(body_length as usize);
            frame.body = body;
        }
        Ok(Some(frame))
    }

    fn read_i32(src: &mut Cursor<&[u8]>) -> Result<i32, Error> {
        if src.remaining() < 4 {
            return Err(Error::Incomplete);
        }
        Ok(src.get_i32())
    }

    pub(crate) fn encode(&self) -> Result<Option<Bytes>, ClientError> {
        let header = serde_json::to_vec(self).map_err(|_e| {
            return ClientError::InvalidFrame("Failed to JSON serialize frame header".to_string());
        })?;
        let len = 4 + header.len() + self.body.len();
        let mut buf = BytesMut::with_capacity(len);
        buf.put_i32(len as i32);
        buf.put_i32(header.len() as i32);
        buf.put_slice(&header);
        buf.put_slice(&self.body);
        Ok(Some(buf.into()))
    }

    pub(crate) fn put_ext_field(&mut self, key: &str, value: &str) {
        self.ext_fields.insert(key.to_owned(), value.to_owned());
    }

    pub(crate) fn remark(&self) -> &str {
        self.remark.as_str()
    }

    pub(crate) fn frame_type(&self) -> Type {
        if self.flag & 1 == 1 {
            return Type::Response;
        }
        Type::Request
    }

    pub(crate) fn mark_response_type(&mut self) {
        self.flag |= 1;
    }

    pub(crate) fn add_ext_headers(&mut self, header: impl Into<HashMap<String, String>>) {
        let map: HashMap<String, String> = header.into();
        map.iter().for_each(|(k, v)| {
            self.put_ext_field(k, v);
        });
    }

    pub(crate) fn body(&self) -> bytes::Bytes {
        self.body.clone()
    }

}

mod tests {
    use bytes::{Buf, BufMut, BytesMut};

    use super::{Frame, Language, Type};

    #[test]
    fn test_new() {
        let frame_0 = Frame::new();
        let frame_1 = Frame::new();
        assert_eq!(frame_0.opaque < frame_1.opaque, true);
    }

    #[test]
    fn test_deserialization() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"
        {"code": 1, "language": "JAVA", "version": 0, "opaque": 0, "flag": 0}
        "#;
        let frame: Frame = serde_json::from_str(json)?;
        assert_eq!(1, frame.code);
        assert_eq!(Language::JAVA, frame.language);
        Ok(())
    }

    #[test]
    fn test_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let mut frame = Frame::new();
        frame
            .ext_fields
            .insert("key".to_string(), "value".to_string());
        let json = serde_json::to_string(&frame)?;
        println!("json={}", json);
        let frame2 = serde_json::from_str(&json)?;
        assert_eq!(frame, frame2);
        Ok(())
    }

    #[test]
    fn test_deserialize_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = BytesMut::with_capacity(1024);
        let data = r#"{"code": 1, "language": "JAVA", "version": 0, "opaque": 0, "flag": 0}"#;
        buf.put(data.as_bytes());
        let frame: Frame = serde_json::from_reader(buf.reader())?;
        assert_eq!(frame.language, Language::JAVA);
        assert_eq!(frame.code, 1);
        assert_eq!(frame.opaque, 0);
        assert_eq!(frame.version, 0);
        assert_eq!(frame.flag, 0);
        assert_eq!(frame.ext_fields.is_empty(), true);
        Ok(())
    }

    #[test]
    fn test_type() {
        let mut frame = Frame::new();
        assert_eq!(frame.frame_type(), Type::Request);

        frame.mark_response_type();
        assert_eq!(frame.frame_type(), Type::Response);
    }

    #[test]
    fn test_add_ext_headers() {
        let header = crate::protocol::GetRouteInfoRequestHeader::new("Test");
        let mut frame = Frame::new();
        frame.add_ext_headers(header);
        assert_eq!(frame.ext_fields.len(), 1);
    }

}
