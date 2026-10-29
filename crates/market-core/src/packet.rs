use anyhow::{Result, bail, ensure};
use serde::Serialize;

use crate::MarketEvent;

const FULL_PACKET_SIZE: usize = 162;
const DEPTH_START: usize = 62;
const DEPTH_LEVEL_SIZE: usize = 20;
const DEPTH_LEVELS: usize = 5;
const MCX_SEGMENT_CODE: u8 = 5;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LatestQuote {
    pub last_price: f32,
    pub last_trade_quantity: u16,
    pub average_price: f32,
    pub volume_raw: u32,
    pub total_sell_quantity: u32,
    pub total_buy_quantity: u32,
    pub open_interest: u32,
    pub open: f32,
    pub close: f32,
    pub high: f32,
    pub low: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DepthLevel {
    pub bid_quantity: u32,
    pub ask_quantity: u32,
    pub bid_orders: u16,
    pub ask_orders: u16,
    pub bid_price: f32,
    pub ask_price: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FiveLevelDepth {
    pub levels: [DepthLevel; DEPTH_LEVELS],
}

pub(crate) fn decode_full_event(event: &MarketEvent) -> Result<(LatestQuote, FiveLevelDepth)> {
    ensure!(event.exchange_code == "MCX", "event is not from MCX");
    ensure!(
        event.packet_type == 8,
        "unsupported packet type {}; Q003 accepts only verified full packets (8)",
        event.packet_type
    );
    ensure!(
        event.raw_packet.len() == FULL_PACKET_SIZE,
        "full packet length is {}, expected {FULL_PACKET_SIZE}",
        event.raw_packet.len()
    );
    ensure!(
        event.raw_packet[3] == MCX_SEGMENT_CODE,
        "raw packet exchange segment is {}, expected MCX code {MCX_SEGMENT_CODE}",
        event.raw_packet[3]
    );

    let quote = LatestQuote {
        last_price: float32(&event.raw_packet, 8),
        last_trade_quantity: u16_value(&event.raw_packet, 12),
        average_price: float32(&event.raw_packet, 18),
        volume_raw: u32_value(&event.raw_packet, 22),
        total_sell_quantity: u32_value(&event.raw_packet, 26),
        total_buy_quantity: u32_value(&event.raw_packet, 30),
        open_interest: u32_value(&event.raw_packet, 34),
        open: float32(&event.raw_packet, 46),
        close: float32(&event.raw_packet, 50),
        high: float32(&event.raw_packet, 54),
        low: float32(&event.raw_packet, 58),
    };
    for (name, value) in [
        ("last_price", quote.last_price),
        ("average_price", quote.average_price),
        ("open", quote.open),
        ("close", quote.close),
        ("high", quote.high),
        ("low", quote.low),
    ] {
        if !value.is_finite() {
            bail!("decoded {name} is not finite");
        }
    }

    let levels = std::array::from_fn(|index| {
        let offset = DEPTH_START + index * DEPTH_LEVEL_SIZE;
        DepthLevel {
            bid_quantity: u32_value(&event.raw_packet, offset),
            ask_quantity: u32_value(&event.raw_packet, offset + 4),
            bid_orders: u16_value(&event.raw_packet, offset + 8),
            ask_orders: u16_value(&event.raw_packet, offset + 10),
            bid_price: float32(&event.raw_packet, offset + 12),
            ask_price: float32(&event.raw_packet, offset + 16),
        }
    });
    for (index, level) in levels.iter().enumerate() {
        ensure!(
            level.bid_price.is_finite() && level.ask_price.is_finite(),
            "decoded depth level {index} contains a non-finite price"
        );
    }
    Ok((quote, FiveLevelDepth { levels }))
}

fn u16_value(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn u32_value(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn float32(bytes: &[u8], offset: usize) -> f32 {
    f32::from_bits(u32_value(bytes, offset))
}
