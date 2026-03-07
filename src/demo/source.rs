//! Demo file source for static `.dem` files.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::session::{IntoStreamingSession, Schemas, SessionError, StreamingSession};
use crate::visitor::discover_schemas_from_demo;

pub struct DemoSource {
    bytes: Bytes,
}

impl DemoSource {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, SessionError> {
        let bytes = tokio::fs::read(path).await?;
        Ok(Self::from_bytes(bytes))
    }

    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        Self {
            bytes: bytes.into(),
        }
    }
}

#[async_trait]
impl IntoStreamingSession for DemoSource {
    async fn into_session(self) -> Result<(StreamingSession, Schemas), SessionError> {
        let schema_vec = discover_schemas_from_demo(&self.bytes)
            .await
            .map_err(|e| SessionError::Schema(e.to_string()))?;

        let schemas: Schemas = schema_vec
            .into_iter()
            .map(|s| (Arc::clone(&s.serializer_name), s))
            .collect();

        let session = StreamingSession::from_demo_bytes_internal(self.bytes, schemas.clone());

        Ok((session, schemas))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_demo_source_from_bytes() {
        let bytes = vec![0u8; 100];
        let source = DemoSource::from_bytes(bytes);
        assert_eq!(source.bytes.len(), 100);
    }

    #[test]
    fn test_demo_source_from_bytes_ref() {
        let bytes: &[u8] = &[1, 2, 3, 4, 5];
        let source = DemoSource::from_bytes(Bytes::from_static(bytes));
        assert_eq!(source.bytes.len(), 5);
    }

    #[tokio::test]
    async fn test_demo_source_open_nonexistent_file() {
        let result = DemoSource::open("/nonexistent/path/to/demo.dem").await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, SessionError::Io(_)));
    }

    #[tokio::test]
    async fn test_demo_source_into_session_invalid_bytes() {
        let source = DemoSource::from_bytes(vec![0u8; 100]);
        let result = source.into_session().await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, SessionError::Schema(_)));
    }

    #[tokio::test]
    #[ignore = "requires demo file: set TEST_DEMO_PATH env var"]
    async fn test_demo_source_open_and_into_session() {
        let demo_path =
            std::env::var("TEST_DEMO_PATH").expect("TEST_DEMO_PATH must be set to run this test");

        let source = DemoSource::open(&demo_path)
            .await
            .expect("Failed to open demo");
        let (session, schemas) = source
            .into_session()
            .await
            .expect("Failed to create session");

        assert!(!schemas.is_empty(), "Should discover at least one schema");
        assert!(
            schemas.contains_key("CCitadelPlayerPawn"),
            "Should contain CCitadelPlayerPawn entity"
        );
        assert!(!session.entity_names().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires demo file: set TEST_DEMO_PATH env var"]
    async fn test_demo_source_query_execution() {
        use futures::StreamExt;

        let demo_path =
            std::env::var("TEST_DEMO_PATH").expect("TEST_DEMO_PATH must be set to run this test");

        let source = DemoSource::open(&demo_path)
            .await
            .expect("Failed to open demo");
        let (mut session, _schemas) = source
            .into_session()
            .await
            .expect("Failed to create session");

        let mut query = session
            .add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 10")
            .await
            .expect("Failed to add query");

        let _result = session.start().expect("Failed to start session");

        let mut row_count = 0;
        while let Some(batch_result) = query.next().await {
            let batch = batch_result.expect("Query returned error");
            row_count += batch.num_rows();
        }

        assert!(row_count > 0, "Should return at least one row");
        assert!(row_count <= 10, "LIMIT 10 should return at most 10 rows");
    }
}
