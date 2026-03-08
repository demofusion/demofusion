//! GOTV HTTP Broadcast client.
//!
//! Implements Valve's GOTV broadcast protocol for streaming live match data.
//!
//! # Protocol Overview
//!
//! The broadcast protocol follows a state machine:
//! ```text
//! START -> fetch /{signup_fragment}/start (schema/string tables)
//! FULLFRAME -> fetch /{fragment}/full (keyframe snapshot)
//! DELTAFRAMES -> fetch /{fragment}/delta (incremental updates, loops)
//! STOP -> broadcast ended
//! ```
//!
//! # Example
//!
//! ```ignore
//! use demofusion::gotv::{BroadcastClient, ClientConfig};
//! use tokio::sync::mpsc;
//!
//! let mut client = BroadcastClient::new("http://dist1-ord1.steamcontent.com/tv/18895867");
//!
//! // Sync to get broadcast metadata
//! let sync = client.sync().await?;
//! println!("Map: {}, fragment: {}", sync.map, sync.fragment);
//!
//! // Fetch start packet for schema discovery
//! let start_packet = client.fetch_start().await?;
//!
//! // Stream packets to a channel
//! let (tx, rx) = mpsc::channel(32);
//! let result = client.stream_to_channel(tx).await?;
//! ```

use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use super::config::ClientConfig;
use super::error::GotvError;

const DEADLOCK_APP_ID: u32 = 1422450;

/// Response from the /sync endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct SyncResponse {
    /// Start tick of the current fragment.
    pub tick: i64,
    /// End tick.
    pub endtick: i64,
    /// Maximum tick.
    pub maxtick: i64,
    /// Delay from real-time in seconds.
    pub rtdelay: f64,
    /// Seconds since relay received data from game server.
    pub rcvage: f64,
    /// Current fragment number.
    pub fragment: u64,
    /// Signup fragment number (used for /start endpoint).
    pub signup_fragment: u64,
    /// Ticks per second.
    pub tps: u32,
    /// Interval between keyframes in seconds.
    pub keyframe_interval: u32,
    /// Map name.
    pub map: String,
    /// Protocol version.
    pub protocol: u32,
}

/// Statistics about a broadcast stream.
#[derive(Debug, Default, Clone)]
pub struct BroadcastStats {
    /// Number of fragments successfully received.
    pub fragments: u64,
    /// Number of fragments skipped due to errors.
    pub fragments_skipped: u64,
    /// Total bytes downloaded.
    pub bytes_downloaded: u64,
}

/// Result of streaming a broadcast to completion.
pub struct StreamResult {
    /// Final statistics.
    pub stats: BroadcastStats,
    /// Reason the stream ended.
    pub reason: StreamEndReason,
}

/// Why the broadcast stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamEndReason {
    /// Broadcast ended normally (no new fragments).
    BroadcastEnded,
    /// Stream was cancelled via cancellation token.
    Cancelled,
    /// Receiving channel was closed.
    ChannelClosed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamState {
    Start,
    FullFrame,
    DeltaFrames,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FragmentType {
    Full,
    Delta,
}

impl FragmentType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Delta => "delta",
        }
    }
}

/// GOTV HTTP Broadcast client.
///
/// Streams demo packets from a live GOTV broadcast.
pub struct BroadcastClient {
    base_url: String,
    client: Client,
    config: ClientConfig,
    cancel_token: Option<CancellationToken>,

    state: StreamState,
    sync_response: Option<SyncResponse>,
    stream_fragment: u64,
    keyframe_interval: Duration,
    signup_fragment: u64,
    start_packet: Option<Bytes>,

    stats: BroadcastStats,
}

impl BroadcastClient {
    /// Create a new client with default configuration.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_config(base_url, ClientConfig::default())
    }

    /// Create a new client with custom configuration.
    pub fn with_config(base_url: impl Into<String>, config: ClientConfig) -> Self {
        let client = Client::builder()
            .default_headers(Self::default_headers())
            .timeout(config.request_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client,
            config,
            cancel_token: None,
            state: StreamState::Start,
            sync_response: None,
            stream_fragment: 0,
            keyframe_interval: Duration::from_secs(1),
            signup_fragment: 0,
            start_packet: None,
            stats: BroadcastStats::default(),
        }
    }

    /// Attach a cancellation token for graceful shutdown.
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    fn default_headers() -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::USER_AGENT,
            format!("Valve/Steam HTTP Client 1.0 ({})", DEADLOCK_APP_ID)
                .parse()
                .unwrap(),
        );
        headers.insert(
            reqwest::header::ACCEPT,
            "text/html,*/*;q=0.9".parse().unwrap(),
        );
        headers.insert(
            reqwest::header::ACCEPT_ENCODING,
            "gzip,identity,*;q=0".parse().unwrap(),
        );
        headers.insert(
            reqwest::header::ACCEPT_CHARSET,
            "ISO-8859-1,utf-8,*;q=0.7".parse().unwrap(),
        );
        headers
    }

    /// Get the current stream state.
    pub fn state(&self) -> &'static str {
        match self.state {
            StreamState::Start => "start",
            StreamState::FullFrame => "fullframe",
            StreamState::DeltaFrames => "deltaframes",
            StreamState::Stop => "stop",
        }
    }

    /// Get the sync response, if sync() has been called.
    pub fn sync_response(&self) -> Option<&SyncResponse> {
        self.sync_response.as_ref()
    }

    /// Get current statistics.
    pub fn stats(&self) -> &BroadcastStats {
        &self.stats
    }

    /// Get the cached start packet, if fetch_start() has been called.
    pub fn start_packet(&self) -> Option<&Bytes> {
        self.start_packet.as_ref()
    }

    /// Check if cancelled.
    fn is_cancelled(&self) -> bool {
        self.cancel_token.as_ref().is_some_and(|t| t.is_cancelled())
    }

    /// Sleep with cancellation support.
    async fn sleep_cancellable(&self, duration: Duration) -> Result<(), GotvError> {
        match &self.cancel_token {
            Some(token) => {
                tokio::select! {
                    _ = tokio::time::sleep(duration) => Ok(()),
                    _ = token.cancelled() => Err(GotvError::Cancelled),
                }
            }
            None => {
                tokio::time::sleep(duration).await;
                Ok(())
            }
        }
    }

    // URL builders

    fn build_sync_url(&self) -> String {
        format!("{}/sync", self.base_url)
    }

    fn build_start_url(&self) -> String {
        format!("{}/{}/start", self.base_url, self.signup_fragment)
    }

    fn build_fragment_url(&self, fragment: u64, fragment_type: FragmentType) -> String {
        format!("{}/{}/{}", self.base_url, fragment, fragment_type.as_str())
    }

    /// Fetch /sync to get broadcast metadata.
    ///
    /// Retries with exponential backoff until timeout.
    pub async fn sync(&mut self) -> Result<&SyncResponse, GotvError> {
        let url = self.build_sync_url();
        let start_time = Instant::now();
        let mut attempt = 0u32;

        debug!(target: "demofusion::gotv", %url, "syncing broadcast");

        loop {
            if self.is_cancelled() {
                return Err(GotvError::Cancelled);
            }

            if start_time.elapsed() >= self.config.sync_timeout {
                warn!(target: "demofusion::gotv", timeout_secs = ?self.config.sync_timeout, attempts = attempt, "sync timeout");
                return Err(GotvError::BroadcastNotReady);
            }

            attempt += 1;

            let result = self.client.get(&url).send().await;

            let response = match result {
                Ok(resp) => resp,
                Err(e) if e.is_timeout() || e.is_connect() || e.is_request() => {
                    let delay = self.config.backoff_delay(attempt);
                    debug!(target: "demofusion::gotv", attempt, error = %e, delay_ms = delay.as_millis(), "sync request failed, retrying");
                    self.sleep_cancellable(delay).await?;
                    continue;
                }
                Err(e) => return Err(GotvError::Http(e)),
            };

            if !response.status().is_success() {
                let delay = self.config.backoff_delay(attempt);
                debug!(target: "demofusion::gotv", attempt, status = %response.status(), delay_ms = delay.as_millis(), "sync non-success status, retrying");
                self.sleep_cancellable(delay).await?;
                continue;
            }

            let sync: SyncResponse = match response.json().await {
                Ok(s) => s,
                Err(_) => {
                    let delay = self.config.backoff_delay(attempt);
                    self.sleep_cancellable(delay).await?;
                    continue;
                }
            };

            debug!(target: "demofusion::gotv", 
                map = %sync.map, 
                fragment = sync.fragment, 
                tick = sync.tick, 
                "sync complete"
            );

            self.stream_fragment = sync.fragment;
            self.keyframe_interval = Duration::from_secs(sync.keyframe_interval as u64);
            self.signup_fragment = sync.signup_fragment;
            self.sync_response = Some(sync);

            return Ok(self.sync_response.as_ref().unwrap());
        }
    }

    /// Fetch the /start packet containing schema data.
    ///
    /// This packet contains DEM_SendTables needed for schema discovery.
    /// The packet is cached, so subsequent calls return the cached value.
    ///
    /// Retries with exponential backoff until timeout.
    pub async fn fetch_start(&mut self) -> Result<Bytes, GotvError> {
        if self.sync_response.is_none() {
            self.sync().await?;
        }

        if let Some(ref packet) = self.start_packet {
            debug!(target: "demofusion::gotv", bytes = packet.len(), "returning cached start packet");
            return Ok(packet.clone());
        }

        let url = self.build_start_url();
        let start_time = Instant::now();
        let mut attempt = 0u32;

        debug!(target: "demofusion::gotv", %url, "fetching start packet");

        loop {
            if self.is_cancelled() {
                return Err(GotvError::Cancelled);
            }

            if start_time.elapsed() >= self.config.start_timeout {
                warn!(target: "demofusion::gotv", timeout_secs = ?self.config.start_timeout, attempts = attempt, "start packet timeout");
                return Err(GotvError::BroadcastNotReady);
            }

            attempt += 1;

            let result = self.client.get(&url).send().await;

            let response = match result {
                Ok(resp) => resp,
                Err(e) if e.is_timeout() || e.is_connect() || e.is_request() => {
                    let delay = self.config.backoff_delay(attempt);
                    debug!(target: "demofusion::gotv", attempt, error = %e, delay_ms = delay.as_millis(), "start request failed, retrying");
                    self.sleep_cancellable(delay).await?;
                    continue;
                }
                Err(e) => return Err(GotvError::Http(e)),
            };

            if !response.status().is_success() {
                let delay = self.config.backoff_delay(attempt);
                debug!(target: "demofusion::gotv", attempt, status = %response.status(), delay_ms = delay.as_millis(), "start non-success status, retrying");
                self.sleep_cancellable(delay).await?;
                continue;
            }

            let content = response.bytes().await?;
            debug!(target: "demofusion::gotv", bytes = content.len(), "received start packet");
            self.stats.bytes_downloaded += content.len() as u64;
            self.start_packet = Some(content.clone());

            return Ok(content);
        }
    }

    /// Stream all packets to a channel.
    ///
    /// This consumes the client and streams packets until the broadcast ends,
    /// the channel is closed, or cancellation is requested.
    pub async fn stream_to_channel(
        mut self,
        tx: mpsc::Sender<Bytes>,
    ) -> Result<StreamResult, GotvError> {
        if self.sync_response.is_none() {
            self.sync().await?;
        }

        debug!(target: "demofusion::gotv", state = %self.state(), "stream_to_channel starting");

        let reason = loop {
            if self.is_cancelled() {
                debug!(target: "demofusion::gotv", "stream_to_channel cancelled");
                break StreamEndReason::Cancelled;
            }

            match self.fetch_next().await {
                Ok(Some(packet)) => {
                    trace!(target: "demofusion::gotv", bytes = packet.len(), state = %self.state(), "sending packet");
                    if tx.send(packet).await.is_err() {
                        debug!(target: "demofusion::gotv", "channel closed");
                        break StreamEndReason::ChannelClosed;
                    }
                }
                Ok(None) => {
                    trace!(target: "demofusion::gotv", state = %self.state(), "fetch_next returned None");
                    continue;
                }
                Err(GotvError::BroadcastEnded) => {
                    info!(target: "demofusion::gotv", "broadcast ended");
                    break StreamEndReason::BroadcastEnded;
                }
                Err(e) => {
                    error!(target: "demofusion::gotv", error = %e, "stream error");
                    return Err(e);
                }
            }
        };

        debug!(target: "demofusion::gotv", ?reason, bytes_downloaded = self.stats.bytes_downloaded, fragments = self.stats.fragments, "stream_to_channel finished");
        Ok(StreamResult {
            stats: self.stats,
            reason,
        })
    }

    async fn fetch_next(&mut self) -> Result<Option<Bytes>, GotvError> {
        match self.state {
            StreamState::Start => self.handle_start().await,
            StreamState::FullFrame => self.handle_fullframe().await,
            StreamState::DeltaFrames => self.handle_deltaframes().await,
            StreamState::Stop => Ok(None),
        }
    }

    async fn handle_start(&mut self) -> Result<Option<Bytes>, GotvError> {
        // If already fetched via fetch_start(), use cached packet
        if let Some(ref packet) = self.start_packet {
            debug!(target: "demofusion::gotv", bytes = packet.len(), "returning cached start packet");
            self.state = StreamState::FullFrame;
            return Ok(Some(packet.clone()));
        }

        let url = self.build_start_url();
        debug!(target: "demofusion::gotv", %url, "fetching start packet");
        let response = self.client.get(&url).send().await?;
        response.error_for_status_ref()?;

        let content = response.bytes().await?;
        debug!(target: "demofusion::gotv", bytes = content.len(), "received start packet");
        self.stats.bytes_downloaded += content.len() as u64;
        self.start_packet = Some(content.clone());
        self.state = StreamState::FullFrame;

        Ok(Some(content))
    }

    async fn handle_fullframe(&mut self) -> Result<Option<Bytes>, GotvError> {
        let url = self.build_fragment_url(self.stream_fragment, FragmentType::Full);
        debug!(target: "demofusion::gotv", %url, fragment = self.stream_fragment, "fetching fullframe");

        let response = self.client.get(&url).send().await?;
        response.error_for_status_ref()?;

        let content = response.bytes().await?;
        debug!(target: "demofusion::gotv", bytes = content.len(), "received fullframe");
        self.stats.bytes_downloaded += content.len() as u64;
        self.stats.fragments += 1;
        self.state = StreamState::DeltaFrames;

        Ok(Some(content))
    }

    async fn handle_deltaframes(&mut self) -> Result<Option<Bytes>, GotvError> {
        let mut retries = 0;

        while retries < self.config.delta_retries {
            if self.is_cancelled() {
                debug!(target: "demofusion::gotv", "delta fetch cancelled");
                return Ok(None);
            }

            let url = self.build_fragment_url(self.stream_fragment, FragmentType::Delta);
            trace!(target: "demofusion::gotv", %url, "fetching delta");

            let response = match self.client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    warn!(target: "demofusion::gotv", error = %e, "delta request error");
                    return Err(GotvError::Http(e));
                }
            };

            trace!(target: "demofusion::gotv", status = %response.status(), "delta response");

            if response.status() == reqwest::StatusCode::NOT_FOUND {
                retries += 1;
                debug!(target: "demofusion::gotv", retries, max_retries = self.config.delta_retries, "delta 404, retrying");
                self.sleep_cancellable(self.keyframe_interval).await?;
                continue;
            }

            if let Err(e) = response.error_for_status_ref() {
                warn!(target: "demofusion::gotv", error = %e, "delta HTTP error");
                return Err(GotvError::Http(e));
            }

            let content = match response.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    warn!(target: "demofusion::gotv", error = %e, "delta body error, skipping fragment");
                    self.stats.fragments_skipped += 1;
                    self.stream_fragment += 1;
                    return Ok(None);
                }
            };

            trace!(target: "demofusion::gotv", bytes = content.len(), fragment = self.stream_fragment, "received delta");
            self.stats.bytes_downloaded += content.len() as u64;
            self.stats.fragments += 1;
            self.stream_fragment += 1;

            return Ok(Some(content));
        }

        info!(target: "demofusion::gotv", "max retries exceeded, broadcast ended");
        self.state = StreamState::Stop;
        Err(GotvError::BroadcastEnded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = BroadcastClient::new("http://example.com/tv/12345");
        assert_eq!(client.base_url, "http://example.com/tv/12345");
        assert_eq!(client.state(), "start");
    }

    #[test]
    fn test_client_with_config() {
        let config = ClientConfig::builder().delta_retries(10).build();

        let client = BroadcastClient::with_config("http://example.com", config);
        assert_eq!(client.config.delta_retries, 10);
    }

    #[test]
    fn test_url_builders() {
        let mut client = BroadcastClient::new("http://example.com/tv/12345/");
        client.signup_fragment = 100;
        client.stream_fragment = 150;

        assert_eq!(client.build_sync_url(), "http://example.com/tv/12345/sync");
        assert_eq!(
            client.build_start_url(),
            "http://example.com/tv/12345/100/start"
        );
        assert_eq!(
            client.build_fragment_url(150, FragmentType::Full),
            "http://example.com/tv/12345/150/full"
        );
        assert_eq!(
            client.build_fragment_url(150, FragmentType::Delta),
            "http://example.com/tv/12345/150/delta"
        );
    }
}
