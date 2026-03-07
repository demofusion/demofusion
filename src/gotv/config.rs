//! Configuration for GOTV broadcast client.

use std::time::Duration;

/// Configuration for the GOTV broadcast client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Max retries for delta fragments before considering broadcast ended.
    pub delta_retries: u32,

    /// Timeout for sync endpoint retries.
    pub sync_timeout: Duration,

    /// Timeout for start endpoint retries.
    pub start_timeout: Duration,

    /// Base delay for exponential backoff.
    pub backoff_base: Duration,

    /// Maximum backoff delay.
    pub backoff_max: Duration,

    /// HTTP request timeout.
    pub request_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            delta_retries: 5,
            sync_timeout: Duration::from_secs(30),
            start_timeout: Duration::from_secs(30),
            backoff_base: Duration::from_millis(500),
            backoff_max: Duration::from_secs(4),
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl ClientConfig {
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder::default()
    }

    pub(crate) fn backoff_delay(&self, attempt: u32) -> Duration {
        let multiplier = 1u32 << (attempt.saturating_sub(1)).min(4);
        let delay = self.backoff_base * multiplier;
        delay.min(self.backoff_max)
    }
}

#[derive(Debug, Default)]
pub struct ClientConfigBuilder {
    config: ClientConfig,
}

impl ClientConfigBuilder {
    pub fn delta_retries(mut self, n: u32) -> Self {
        self.config.delta_retries = n;
        self
    }

    pub fn sync_timeout(mut self, d: Duration) -> Self {
        self.config.sync_timeout = d;
        self
    }

    pub fn start_timeout(mut self, d: Duration) -> Self {
        self.config.start_timeout = d;
        self
    }

    pub fn backoff_base(mut self, d: Duration) -> Self {
        self.config.backoff_base = d;
        self
    }

    pub fn backoff_max(mut self, d: Duration) -> Self {
        self.config.backoff_max = d;
        self
    }

    pub fn request_timeout(mut self, d: Duration) -> Self {
        self.config.request_timeout = d;
        self
    }

    pub fn build(self) -> ClientConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ClientConfig::default();
        assert_eq!(config.delta_retries, 5);
        assert_eq!(config.sync_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_builder() {
        let config = ClientConfig::builder()
            .delta_retries(10)
            .sync_timeout(Duration::from_secs(60))
            .build();

        assert_eq!(config.delta_retries, 10);
        assert_eq!(config.sync_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_backoff_delay() {
        let config = ClientConfig::default();

        assert_eq!(config.backoff_delay(1), Duration::from_millis(500));
        assert_eq!(config.backoff_delay(2), Duration::from_millis(1000));
        assert_eq!(config.backoff_delay(3), Duration::from_millis(2000));
        assert_eq!(config.backoff_delay(4), Duration::from_millis(4000));
        assert_eq!(config.backoff_delay(5), Duration::from_millis(4000)); // capped
        assert_eq!(config.backoff_delay(100), Duration::from_millis(4000)); // capped
    }
}
