//! RecordBatchStream wrapper for DistributionReceiver.
//!
//! This provides a simple adapter from `DistributionReceiver<RecordBatch>` to
//! DataFusion's `RecordBatchStream` trait. The distribution channel's gate-based
//! backpressure prevents JOIN deadlocks without the complexity of tick watermarks.

use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use tracing::{debug, error, trace};

use super::distributor_channels::DistributionReceiver;
use super::stream_fault::StreamFault;

pub struct DistributionReceiverStream {
    schema: SchemaRef,
    receiver: DistributionReceiver<RecordBatch>,
    /// Set by the producer when this channel's data is incomplete. Checked at
    /// end-of-stream so a truncated stream errors instead of ending cleanly.
    fault: StreamFault,
    finished: bool,
}

impl DistributionReceiverStream {
    /// A stream that can never report a fault. For tests and callers that own
    /// both ends; production streams go through [`Self::with_fault`].
    pub fn new(schema: SchemaRef, receiver: DistributionReceiver<RecordBatch>) -> Self {
        Self::with_fault(schema, receiver, StreamFault::new())
    }

    pub fn with_fault(
        schema: SchemaRef,
        receiver: DistributionReceiver<RecordBatch>,
        fault: StreamFault,
    ) -> Self {
        Self {
            schema,
            receiver,
            fault,
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
                self.finished = true;

                // The senders are gone. Whether that means "the demo ended" or
                // "the producer died half way through" is not visible on the
                // channel itself, so ask the fault slot. Reporting the error
                // here is what keeps a truncated stream from masquerading as a
                // complete one.
                if let Some(reason) = self.fault.get() {
                    error!(
                        target: "demofusion::stream",
                        %reason,
                        "poll_next: stream ended early, surfacing producer fault"
                    );
                    return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                        "demo stream truncated: {reason}"
                    )))));
                }

                debug!(target: "demofusion::stream", "poll_next: stream ended (senders dropped)");
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
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
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

    #[tokio::test]
    async fn test_stream_with_unset_fault_still_ends_cleanly() {
        // Intentional early termination — every consumer satisfied, senders
        // dropped, no fault recorded — must still look like a normal end of
        // stream. Regressing this would turn every satisfied LIMIT query into
        // a failure.
        use crate::datafusion::distributor_channels::channels;

        let schema = make_test_schema();
        let (senders, mut receivers) = channels::<RecordBatch>(1);

        senders[0]
            .send(make_test_batch(&schema, &[1, 2]))
            .await
            .unwrap();
        drop(senders);

        let mut stream = DistributionReceiverStream::with_fault(
            schema,
            receivers.pop().unwrap(),
            StreamFault::new(),
        );

        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 2);
        assert!(
            stream.next().await.is_none(),
            "a stream with no fault recorded must end cleanly"
        );
    }

    #[tokio::test]
    async fn test_stream_reports_fault_instead_of_ending_cleanly() {
        // The whole point of the fault channel: the producer died part way
        // through, so the consumer must see an error rather than an ordinary
        // end of stream that is indistinguishable from a demo running out.
        use crate::datafusion::distributor_channels::channels;

        let schema = make_test_schema();
        let (senders, mut receivers) = channels::<RecordBatch>(1);
        let fault = StreamFault::new();

        senders[0]
            .send(make_test_batch(&schema, &[1, 2, 3]))
            .await
            .unwrap();

        // The producer records why it is going away *before* dropping its
        // senders, which is the ordering the parser task guarantees.
        fault.set("field path not found");
        drop(senders);

        let mut stream =
            DistributionReceiverStream::with_fault(schema, receivers.pop().unwrap(), fault);

        // The rows produced before the failure are real and still delivered.
        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 3);

        let err = stream
            .next()
            .await
            .expect("a faulted stream must yield an error, not None")
            .expect_err("the terminal item must be an error");
        assert!(
            err.to_string().contains("field path not found"),
            "the error must carry the reason, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_faulted_stream_does_not_repeat_its_error() {
        // The error is terminal: polling past it yields None, so a consumer
        // that keeps polling does not spin on the same failure forever.
        use crate::datafusion::distributor_channels::channels;

        let schema = make_test_schema();
        let (senders, mut receivers) = channels::<RecordBatch>(1);
        let fault = StreamFault::new();
        fault.set("decode failed");
        drop(senders);

        let mut stream =
            DistributionReceiverStream::with_fault(schema, receivers.pop().unwrap(), fault);

        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
    }
}
