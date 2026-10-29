use std::io::Write;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use goldlab_market_core::MarketEvent;
use serde::Serialize;

use crate::checksum::EventChecksum;
use crate::recording::{ORDERING_POLICY, RecordingReader};

const REPLAY_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReplaySpeed {
    Max,
    WallClock,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct ReplayRow {
    pub sequence: u64,
    pub event_id: String,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub kafka_timestamp_ms: Option<i64>,
    pub message_key_hex: String,
    pub exchange_code: String,
    pub security_id: String,
    pub received_at_ns_utc: i64,
    pub exchange_event_time_epoch_seconds_raw: Option<i64>,
    pub packet_type: u8,
    pub payload_json: String,
    pub raw_packet_hex: String,
}

impl ReplayRow {
    fn from_event(sequence: u64, event: &MarketEvent) -> Result<Self> {
        Ok(Self {
            sequence,
            event_id: event.stable_id(),
            source_topic: event.coordinate.topic.clone(),
            source_partition: event.coordinate.partition,
            source_offset: event.coordinate.offset,
            kafka_timestamp_ms: event.kafka_timestamp_ms,
            message_key_hex: hex::encode(&event.message_key),
            exchange_code: event.exchange_code.clone(),
            security_id: event.security_id.clone(),
            received_at_ns_utc: event.received_at_ns_utc,
            exchange_event_time_epoch_seconds_raw: event.exchange_event_time_epoch_seconds_raw,
            packet_type: event.packet_type,
            payload_json: String::from_utf8(event.payload_json.clone())
                .context("payload_json is not UTF-8")?,
            raw_packet_hex: hex::encode(&event.raw_packet),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct ReplaySummary {
    pub replay_schema_version: u16,
    pub replay_version: String,
    pub manifest_sha256: String,
    pub ordering_policy: String,
    pub speed: ReplaySpeed,
    pub event_count: u64,
    pub partitions: usize,
    pub offset_gaps: u64,
    pub missing_offsets: u64,
    pub received_time_regressions: u64,
    pub checksum_sha256: String,
}

pub struct ReplayEngine {
    speed: ReplaySpeed,
}

impl ReplayEngine {
    #[must_use]
    pub fn new(speed: ReplaySpeed) -> Self {
        Self { speed }
    }

    pub fn run(self, reader: RecordingReader, mut output: impl Write) -> Result<ReplaySummary> {
        let manifest_sha256 = reader.manifest_sha256().to_owned();
        let mut checksum = EventChecksum::new();
        let mut sequence = 0_u64;
        let mut previous_received_at = None;
        let mut received_time_regressions = 0_u64;
        let stats = reader.visit(|event| {
            if let Some(previous) = previous_received_at {
                if event.received_at_ns_utc < previous {
                    received_time_regressions = received_time_regressions.saturating_add(1);
                }
                if self.speed == ReplaySpeed::WallClock
                    && let Some(delay) = pacing_delay(previous, event.received_at_ns_utc)
                {
                    thread::sleep(delay);
                }
            }
            previous_received_at = Some(event.received_at_ns_utc);
            checksum.update(&event);
            let row = ReplayRow::from_event(sequence, &event)?;
            serde_json::to_writer(&mut output, &row).context("encode replay row")?;
            output.write_all(b"\n").context("write replay newline")?;
            sequence = sequence.saturating_add(1);
            Ok(())
        })?;
        output.flush().context("flush replay output")?;
        Ok(ReplaySummary {
            replay_schema_version: REPLAY_SCHEMA_VERSION,
            replay_version: env!("CARGO_PKG_VERSION").to_owned(),
            manifest_sha256,
            ordering_policy: ORDERING_POLICY.to_owned(),
            speed: self.speed,
            event_count: stats.events,
            partitions: stats.partitions,
            offset_gaps: stats.offset_gaps,
            missing_offsets: stats.missing_offsets,
            received_time_regressions,
            checksum_sha256: checksum.finish(),
        })
    }
}

#[must_use]
pub fn pacing_delay(previous_ns: i64, current_ns: i64) -> Option<Duration> {
    let difference = current_ns.checked_sub(previous_ns)?;
    let nanoseconds = u64::try_from(difference).ok()?;
    (nanoseconds > 0).then(|| Duration::from_nanos(nanoseconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pacing_never_waits_for_equal_or_regressing_time() {
        assert_eq!(pacing_delay(10, 10), None);
        assert_eq!(pacing_delay(10, 9), None);
        assert_eq!(pacing_delay(10, 15), Some(Duration::from_nanos(5)));
    }
}
