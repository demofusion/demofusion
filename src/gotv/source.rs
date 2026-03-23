//! GOTV broadcast source for live match streaming.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use super::client::BroadcastClient;
use super::error::GotvError;
use crate::session::{IntoStreamingSession, SessionError, StreamingSession};
use crate::visitor::discover_schemas_from_broadcast;

pub struct GotvSource {
    client: BroadcastClient,
    start_packet: Bytes,
}

impl GotvSource {
    pub async fn connect(url: &str) -> Result<Self, GotvError> {
        let mut client = BroadcastClient::new(url);
        let start_packet = client.fetch_start().await?;
        Ok(Self {
            client,
            start_packet,
        })
    }

    pub fn from_client(client: BroadcastClient, start_packet: Bytes) -> Self {
        Self {
            client,
            start_packet,
        }
    }
}

#[async_trait]
impl IntoStreamingSession for GotvSource {
    async fn into_session(self) -> Result<StreamingSession, SessionError> {
        let schema_vec = discover_schemas_from_broadcast(&self.start_packet)
            .await
            .map_err(|e| SessionError::Schema(e.to_string()))?;

        let schemas: crate::session::Schemas = schema_vec
            .into_iter()
            .map(|s| (Arc::clone(&s.serializer_name), s))
            .collect();

        let session =
            StreamingSession::from_gotv_internal(self.client, schemas, self.start_packet);

        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gotv_source_from_client() {
        let client = BroadcastClient::new("http://example.com/tv/12345");
        let start_packet = Bytes::from_static(&[0u8; 100]);
        let source = GotvSource::from_client(client, start_packet);
        assert_eq!(source.start_packet.len(), 100);
    }

    #[tokio::test]
    async fn test_gotv_source_into_session_invalid_packet() {
        let client = BroadcastClient::new("http://example.com/tv/12345");
        let start_packet = Bytes::from_static(&[0u8; 100]);
        let source = GotvSource::from_client(client, start_packet);

        let result = source.into_session().await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, SessionError::Schema(_)));
    }

    #[tokio::test]
    async fn test_gotv_source_connect_invalid_url() {
        let result = GotvSource::connect("http://localhost:1/nonexistent").await;
        assert!(result.is_err());
    }
}
