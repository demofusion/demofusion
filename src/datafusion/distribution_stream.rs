//! RecordBatchStream wrapper for DistributionReceiver.
//!
//! This provides a simple adapter from `DistributionReceiver<RecordBatch>` to
//! DataFusion's `RecordBatchStream` trait. The distribution channel's gate-based
//! backpressure prevents JOIN deadlocks without the complexity of tick watermarks.

use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DfResult;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use tracing::{trace, debug};

use super::distributor_channels::DistributionReceiver;

pub struct DistributionReceiverStream {
    schema: SchemaRef,
    receiver: DistributionReceiver<RecordBatch>,
    finished: bool,
}

impl DistributionReceiverStream {
    pub fn new(schema: SchemaRef, receiver: DistributionReceiver<RecordBatch>) -> Self {
        Self {
            schema,
            receiver,
            finished: false,
        }
    }
}

impl Stream for DistributionReceiverStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            trace!(target: "demofusion::stream", "poll_next: already finished");
            return Poll::Ready(None);
        }

        let queue_len = self.receiver.queue_len();
        trace!(target: "demofusion::stream", queue_len, "poll_next called");

        let recv_fut = self.receiver.recv();
        futures::pin_mut!(recv_fut);

        match recv_fut.poll(cx) {
            Poll::Ready(Some(batch)) => {
                let num_rows = batch.num_rows();
                debug!(target: "demofusion::stream", num_rows, "poll_next: received batch");
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => {
                debug!(target: "demofusion::stream", "poll_next: stream ended (senders dropped)");
                self.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                trace!(target: "demofusion::stream", "poll_next: pending (waiting for data)");
                Poll::Pending
            }
        }
    }
}

impl RecordBatchStream for DistributionReceiverStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for DistributionReceiverStream {
    fn drop(&mut self) {
        debug!(
            target: "demofusion::stream",
            finished = self.finished,
            "DistributionReceiverStream dropped"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use std::sync::Arc;

    fn make_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]))
    }

    fn make_test_batch(schema: &SchemaRef, values: &[i32]) -> RecordBatch {
        let array = Int32Array::from(values.to_vec());
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }

    #[tokio::test]
    async fn test_stream_receives_batches() {
        use crate::datafusion::distributor_channels::channels;

        let schema = make_test_schema();
        let (senders, mut receivers) = channels::<RecordBatch>(1);

        let batch = make_test_batch(&schema, &[1, 2, 3]);
        senders[0].send(batch.clone()).await.unwrap();
        drop(senders);

        let receiver = receivers.pop().unwrap();
        let mut stream = DistributionReceiverStream::new(schema.clone(), receiver);

        let received = stream.next().await;
        assert!(received.is_some());
        let received_batch = received.unwrap().unwrap();
        assert_eq!(received_batch.num_rows(), 3);

        let end = stream.next().await;
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn test_stream_closes_on_sender_drop() {
        use crate::datafusion::distributor_channels::channels;

        let schema = make_test_schema();
        let (senders, mut receivers) = channels::<RecordBatch>(1);

        drop(senders);

        let receiver = receivers.pop().unwrap();
        let mut stream = DistributionReceiverStream::new(schema, receiver);

        let result = stream.next().await;
        assert!(result.is_none());
    }
}
