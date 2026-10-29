use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const PARQUET_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SourceCoordinate {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SourceRecord {
    pub coordinate: SourceCoordinate,
    pub kafka_timestamp_ms: Option<i64>,
    pub message_key: Vec<u8>,
    pub exchange_code: String,
    pub security_id: String,
    pub received_at_ns_utc: i64,
    pub exchange_event_time_epoch_seconds_raw: Option<i64>,
    pub packet_type: u8,
    pub payload_json: Vec<u8>,
    pub raw_packet: Vec<u8>,
}

#[derive(Deserialize)]
struct WireEvent {
    exchange_code: String,
    security_id: String,
    received_at: String,
    raw_packet: String,
}

impl SourceRecord {
    pub fn from_parts(
        coordinate: SourceCoordinate,
        kafka_timestamp_ms: Option<i64>,
        message_key: &[u8],
        payload: &[u8],
    ) -> Result<Self> {
        if coordinate.offset < 0 {
            bail!("negative Kafka offset {}", coordinate.offset);
        }
        let event: WireEvent = serde_json::from_slice(payload).context("decode event JSON")?;
        if event.exchange_code.is_empty() || event.security_id.is_empty() {
            bail!("event identity fields must not be empty");
        }
        let expected_key = format!("{}:{}", event.exchange_code, event.security_id);
        if message_key != expected_key.as_bytes() {
            bail!(
                "message key {:?} does not match event identity {expected_key:?}",
                String::from_utf8_lossy(message_key)
            );
        }

        let received_at = DateTime::parse_from_rfc3339(&event.received_at)
            .context("parse received_at as RFC3339")?
            .with_timezone(&Utc);
        let received_at_ns_utc = received_at
            .timestamp_nanos_opt()
            .context("received_at is outside nanosecond timestamp range")?;
        let raw_packet = hex::decode(&event.raw_packet).context("decode raw_packet hex")?;
        if raw_packet.len() < 8 {
            bail!("raw_packet is shorter than the 8-byte Dhan header");
        }
        let declared_length = usize::from(u16::from_le_bytes([raw_packet[1], raw_packet[2]]));
        if declared_length != raw_packet.len() {
            bail!(
                "raw_packet length {} does not match declared length {declared_length}",
                raw_packet.len()
            );
        }
        let raw_security_id =
            u32::from_le_bytes([raw_packet[4], raw_packet[5], raw_packet[6], raw_packet[7]])
                .to_string();
        if raw_security_id != event.security_id {
            bail!(
                "raw security ID {raw_security_id} does not match JSON security ID {}",
                event.security_id
            );
        }

        let packet_type = raw_packet[0];
        let exchange_event_time_epoch_seconds_raw = match packet_type {
            2 if raw_packet.len() >= 16 => Some(i64::from(u32::from_le_bytes([
                raw_packet[12],
                raw_packet[13],
                raw_packet[14],
                raw_packet[15],
            ]))),
            4 | 8 if raw_packet.len() >= 18 => Some(i64::from(u32::from_le_bytes([
                raw_packet[14],
                raw_packet[15],
                raw_packet[16],
                raw_packet[17],
            ]))),
            _ => None,
        };

        Ok(Self {
            coordinate,
            kafka_timestamp_ms,
            message_key: message_key.to_vec(),
            exchange_code: event.exchange_code,
            security_id: event.security_id,
            received_at_ns_utc,
            exchange_event_time_epoch_seconds_raw,
            packet_type,
            payload_json: payload.to_vec(),
            raw_packet,
        })
    }

    #[must_use]
    pub fn approximate_size(&self) -> usize {
        self.coordinate.topic.len()
            + self.message_key.len()
            + self.exchange_code.len()
            + self.security_id.len()
            + self.payload_json.len()
            + self.raw_packet.len()
            + 96
    }
}
