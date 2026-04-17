//! Capability requirements derived from the client configuration.
//!
//! `RequiredServices` captures the minimum set of peer service flags that the
//! client needs in order to make progress with its configured sync mode. It is
//! built once from `ClientConfig` at startup and consulted by peer selection to
//! hard-filter incapable peers.

use dashcore::network::constants::ServiceFlags;

use crate::client::config::{ClientConfig, MempoolStrategy};

/// Capabilities a peer must advertise to be usable for this client's sync mode.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct RequiredServices(ServiceFlags);

impl RequiredServices {
    /// Derive the required service flags from the client's configuration.
    ///
    /// `NETWORK` is always required. `COMPACT_FILTERS` is required when
    /// filter sync is enabled. `BLOOM` is required when mempool tracking uses
    /// the BIP37 bloom-filter strategy.
    pub(crate) fn from_config(config: &ClientConfig) -> Self {
        let mut flags = ServiceFlags::NETWORK;
        if config.enable_filters {
            flags |= ServiceFlags::COMPACT_FILTERS;
        }
        if config.enable_mempool_tracking
            && matches!(config.mempool_strategy, MempoolStrategy::BloomFilter)
        {
            flags |= ServiceFlags::BLOOM;
        }
        Self(flags)
    }

    /// Construct directly from raw service flags. Intended for tests.
    #[cfg(test)]
    pub(crate) fn from_flags(flags: ServiceFlags) -> Self {
        Self(flags)
    }

    /// True if `peer_services` covers every required flag.
    pub(crate) fn is_satisfied_by(self, peer_services: ServiceFlags) -> bool {
        peer_services.has(self.0)
    }

    /// Return the underlying flags. Intended for tests.
    #[cfg(test)]
    pub(crate) fn flags(self) -> ServiceFlags {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_always_includes_network() {
        let mut config = ClientConfig::mainnet();
        config.enable_filters = false;
        config.enable_mempool_tracking = false;

        let required = RequiredServices::from_config(&config);
        assert!(required.is_satisfied_by(ServiceFlags::NETWORK));
        assert!(!required.is_satisfied_by(ServiceFlags::NONE));
    }

    #[test]
    fn from_config_adds_compact_filters_when_filters_enabled() {
        let mut config = ClientConfig::mainnet();
        config.enable_filters = true;
        config.enable_mempool_tracking = false;

        let required = RequiredServices::from_config(&config);
        assert!(!required.is_satisfied_by(ServiceFlags::NETWORK));
        assert!(required.is_satisfied_by(ServiceFlags::NETWORK | ServiceFlags::COMPACT_FILTERS));
    }

    #[test]
    fn from_config_adds_bloom_only_for_bloom_mempool_strategy() {
        let mut config = ClientConfig::mainnet();
        config.enable_filters = false;

        config.enable_mempool_tracking = true;
        config.mempool_strategy = MempoolStrategy::FetchAll;
        let fetch_all = RequiredServices::from_config(&config);
        assert!(fetch_all.is_satisfied_by(ServiceFlags::NETWORK));
        assert!(!fetch_all.flags().has(ServiceFlags::BLOOM));

        config.mempool_strategy = MempoolStrategy::BloomFilter;
        let bloom = RequiredServices::from_config(&config);
        assert!(!bloom.is_satisfied_by(ServiceFlags::NETWORK));
        assert!(bloom.is_satisfied_by(ServiceFlags::NETWORK | ServiceFlags::BLOOM));
    }

    #[test]
    fn from_config_skips_bloom_when_mempool_tracking_disabled() {
        let mut config = ClientConfig::mainnet();
        config.enable_filters = false;
        config.enable_mempool_tracking = false;
        config.mempool_strategy = MempoolStrategy::BloomFilter;

        let required = RequiredServices::from_config(&config);
        assert!(!required.flags().has(ServiceFlags::BLOOM));
    }

    #[test]
    fn from_config_combines_all_requirements() {
        let mut config = ClientConfig::mainnet();
        config.enable_filters = true;
        config.enable_mempool_tracking = true;
        config.mempool_strategy = MempoolStrategy::BloomFilter;

        let required = RequiredServices::from_config(&config);
        let full = ServiceFlags::NETWORK | ServiceFlags::COMPACT_FILTERS | ServiceFlags::BLOOM;
        assert!(required.is_satisfied_by(full));
        assert!(!required.is_satisfied_by(ServiceFlags::NETWORK | ServiceFlags::COMPACT_FILTERS));
        assert!(!required.is_satisfied_by(ServiceFlags::NETWORK | ServiceFlags::BLOOM));
    }

    #[test]
    fn preferred_bonus_is_not_required() {
        let config = ClientConfig::mainnet();
        let required = RequiredServices::from_config(&config);
        // NODE_HEADERS_COMPRESSED is a preferred bonus, never in the required set.
        assert!(!required.flags().has(ServiceFlags::NODE_HEADERS_COMPRESSED));
    }
}
