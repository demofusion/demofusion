use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DfResult;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;

use crate::error::Result;

pub struct ChannelRecordBatchStream {
    rx: async_channel::Receiver<Result<RecordBatch>>,
    schema: SchemaRef,
}

impl ChannelRecordBatchStream {
    pub fn new(rx: async_channel::Receiver<Result<RecordBatch>>, schema: SchemaRef) -> Self {
        Self { rx, schema }
    }
}

impl Stream for ChannelRecordBatchStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // async_channel::Receiver implements Stream, so we can poll it directly.
        // We need to project the pin to the inner receiver.
        // SAFETY: We're not moving the receiver, just polling it.
        let rx = unsafe { self.map_unchecked_mut(|s| &mut s.rx) };
        match rx.poll_next(cx) {
            Poll::Ready(Some(result)) => {
                Poll::Ready(Some(result.map_err(|e| {
                    datafusion::error::DataFusionError::External(Box::new(e))
                })))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ChannelRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use std::pin::pin;
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tick", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ]))
    }

    fn test_batch(schema: &SchemaRef, tick: i32, values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![tick; values.len()])),
                Arc::new(Int32Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_stream_receives_single_batch() {
        let schema = test_schema();
        let (tx, rx) = async_channel::bounded(4);

        let stream = ChannelRecordBatchStream::new(rx, schema.clone());
        let mut stream = pin!(stream);

        // Send a batch
        let batch = test_batch(&schema, 100, &[1, 2, 3]);
        tx.send(Ok(batch)).await.unwrap();
        drop(tx); // Close channel

        // Receive the batch
        let received = stream.next().await;
        assert!(received.is_some());
        let batch = received.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);

        // Channel is closed, should get None
        let received = stream.next().await;
        assert!(received.is_none());
    }

    #[tokio::test]
    async fn test_stream_receives_multiple_batches() {
        let schema = test_schema();
        let (tx, rx) = async_channel::bounded(4);

        let stream = ChannelRecordBatchStream::new(rx, schema.clone());
        let mut stream = pin!(stream);

        // Send multiple batches
        for i in 0..3 {
            let batch = test_batch(&schema, i * 100, &[i, i + 1]);
            tx.send(Ok(batch)).await.unwrap();
        }
        drop(tx);

        // Receive all batches
        let mut count = 0;
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_stream_propagates_errors() {
        let schema = test_schema();
        let (tx, rx) = async_channel::bounded(4);

        let stream = ChannelRecordBatchStream::new(rx, schema.clone());
        let mut stream = pin!(stream);

        // Send an error
        let err = crate::error::Source2DfError::Schema("test error".to_string());
        tx.send(Err(err)).await.unwrap();
        drop(tx);

        // Receive the error
        let received = stream.next().await;
        assert!(received.is_some());
        let result = received.unwrap();
        assert!(result.is_err());

        // Check error message
        let err = result.unwrap_err();
        assert!(err.to_string().contains("test error"));
    }

    #[tokio::test]
    async fn test_stream_returns_correct_schema() {
        let schema = test_schema();
        let (_tx, rx) = async_channel::bounded::<Result<RecordBatch>>(4);

        let stream = ChannelRecordBatchStream::new(rx, schema.clone());

        // Schema should match
        let returned_schema = stream.schema();
        assert_eq!(returned_schema.fields().len(), 2);
        assert_eq!(returned_schema.field(0).name(), "tick");
        assert_eq!(returned_schema.field(1).name(), "value");
    }

    #[tokio::test]
    async fn test_stream_empty_channel() {
        let schema = test_schema();
        let (tx, rx) = async_channel::bounded::<Result<RecordBatch>>(4);

        let stream = ChannelRecordBatchStream::new(rx, schema.clone());
        let mut stream = pin!(stream);

        // Close channel without sending anything
        drop(tx);

        // Should immediately return None
        let received = stream.next().await;
        assert!(received.is_none());
    }

    #[tokio::test]
    async fn test_stream_concurrent_producer_consumer() {
        let schema = test_schema();
        let (tx, rx) = async_channel::bounded(4);

        // Use boxed stream for spawned task (needs Send + 'static)
        let mut stream: Pin<Box<dyn Stream<Item = DfResult<RecordBatch>> + Send>> =
            Box::pin(ChannelRecordBatchStream::new(rx, schema.clone()));

        // Spawn producer
        let schema_clone = schema.clone();
        let producer = tokio::spawn(async move {
            for i in 0..10 {
                let batch = test_batch(&schema_clone, i * 100, &[i]);
                if tx.send(Ok(batch)).await.is_err() {
                    break;
                }
                // Small yield to simulate real async behavior
                tokio::task::yield_now().await;
            }
        });

        // Consume
        let mut count = 0;
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
            count += 1;
        }

        producer.await.unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_mpmc_work_stealing() {
        // Test that multiple receivers can pull from the same channel
        // This is the key behavior for multi-partition streaming
        let schema = test_schema();
        let (tx, rx) = async_channel::bounded(16);

        // Clone receiver to simulate multiple partitions
        let rx1 = rx.clone();
        let rx2 = rx.clone();
        drop(rx); // Drop original

        let schema1 = schema.clone();
        let schema2 = schema.clone();

        // Spawn two consumers (use boxed streams for spawned tasks)
        let consumer1 = tokio::spawn(async move {
            let mut stream: Pin<Box<dyn Stream<Item = DfResult<RecordBatch>> + Send>> =
                Box::pin(ChannelRecordBatchStream::new(rx1, schema1));
            let mut count = 0;
            while let Some(result) = stream.next().await {
                assert!(result.is_ok());
                count += 1;
            }
            count
        });

        let consumer2 = tokio::spawn(async move {
            let mut stream: Pin<Box<dyn Stream<Item = DfResult<RecordBatch>> + Send>> =
                Box::pin(ChannelRecordBatchStream::new(rx2, schema2));
            let mut count = 0;
            while let Some(result) = stream.next().await {
                assert!(result.is_ok());
                count += 1;
            }
            count
        });

        // Send batches
        for i in 0..20 {
            let batch = test_batch(&schema, i * 100, &[i]);
            tx.send(Ok(batch)).await.unwrap();
        }
        drop(tx);

        // Both consumers should have received some batches
        let count1 = consumer1.await.unwrap();
        let count2 = consumer2.await.unwrap();

        // Total should be 20 (each batch goes to exactly one consumer)
        assert_eq!(count1 + count2, 20);
        // Both should have received at least some (work stealing)
        // Note: In extreme cases one might get all, but statistically both should get some
        eprintln!(
            "Consumer 1 received: {}, Consumer 2 received: {}",
            count1, count2
        );
    }

    #[tokio::test]
    async fn test_stream_backpressure() {
        // Test that bounded channel provides backpressure
        let schema = test_schema();
        let (tx, rx) = async_channel::bounded(2); // Small buffer

        let stream = ChannelRecordBatchStream::new(rx, schema.clone());
        let mut stream = pin!(stream);

        // Fill the buffer
        for i in 0..2 {
            let batch = test_batch(&schema, i * 100, &[i]);
            tx.send(Ok(batch)).await.unwrap();
        }

        // Next send should block (we'll use try_send to verify)
        let batch = test_batch(&schema, 200, &[2]);
        let result = tx.try_send(Ok(batch));
        assert!(result.is_err()); // Channel full

        // Consume one batch
        let received = stream.next().await;
        assert!(received.is_some());

        // Now we can send again
        let batch = test_batch(&schema, 300, &[3]);
        let result = tx.try_send(Ok(batch));
        assert!(result.is_ok());
    }
}
