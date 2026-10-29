use goldlab_market_core::MarketEvent;
use sha2::{Digest, Sha256};

use crate::recording::ORDERING_POLICY;

pub struct EventChecksum {
    digest: Sha256,
}

impl EventChecksum {
    pub fn new() -> Self {
        let mut checksum = Self {
            digest: Sha256::new(),
        };
        checksum.bytes(b"goldlab-replay-checksum-v1");
        checksum.bytes(ORDERING_POLICY.as_bytes());
        checksum
    }

    pub fn update(&mut self, event: &MarketEvent) {
        self.bytes(event.coordinate.topic.as_bytes());
        self.digest.update(event.coordinate.partition.to_be_bytes());
        self.digest.update(event.coordinate.offset.to_be_bytes());
        self.optional_i64(event.kafka_timestamp_ms);
        self.bytes(&event.message_key);
        self.bytes(event.exchange_code.as_bytes());
        self.bytes(event.security_id.as_bytes());
        self.digest.update(event.received_at_ns_utc.to_be_bytes());
        self.optional_i64(event.exchange_event_time_epoch_seconds_raw);
        self.digest.update([event.packet_type]);
        self.bytes(&event.payload_json);
        self.bytes(&event.raw_packet);
    }

    pub fn finish(self) -> String {
        hex::encode(self.digest.finalize())
    }

    fn optional_i64(&mut self, value: Option<i64>) {
        match value {
            Some(value) => {
                self.digest.update([1]);
                self.digest.update(value.to_be_bytes());
            }
            None => self.digest.update([0]),
        }
    }

    fn bytes(&mut self, value: &[u8]) {
        let length = u64::try_from(value.len()).unwrap_or(u64::MAX);
        self.digest.update(length.to_be_bytes());
        self.digest.update(value);
    }
}
