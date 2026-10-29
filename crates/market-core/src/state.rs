use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use serde::Serialize;

use crate::packet::decode_full_event;
use crate::{
    FiveLevelDepth, InstrumentMetadata, LatestQuote, MARKET_VIEW_SCHEMA_VERSION, MarketEvent,
    SourceCoordinate,
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct SourcePartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketStateConfig {
    pub group_id: String,
    pub stale_after_ns: i64,
    pub instruments: Vec<InstrumentMetadata>,
    pub sources: Vec<SourcePartition>,
}

impl MarketStateConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.group_id.is_empty(),
            "market-state group_id must not be empty"
        );
        ensure!(self.stale_after_ns > 0, "stale_after_ns must be positive");
        ensure!(
            !self.instruments.is_empty(),
            "instrument configuration is empty"
        );
        ensure!(
            !self.sources.is_empty(),
            "source-partition configuration is empty"
        );
        let instrument_count = self
            .instruments
            .iter()
            .map(|instrument| instrument.security_id.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        ensure!(
            instrument_count == self.instruments.len(),
            "instrument configuration contains duplicate security IDs"
        );
        let source_count = self.sources.iter().collect::<BTreeSet<_>>().len();
        ensure!(
            source_count == self.sources.len(),
            "source configuration contains duplicates"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Freshness {
    Missing,
    Fresh { age_ns: i64 },
    Stale { age_ns: i64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IncompleteReason {
    MissingQuote,
    MissingDepth,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InstrumentView {
    metadata: InstrumentMetadata,
    freshness: Freshness,
    incomplete_reasons: Vec<IncompleteReason>,
    last_event_id: Option<String>,
    last_source: Option<SourceCoordinate>,
    received_at_ns_utc: Option<i64>,
    exchange_event_time_epoch_seconds_raw: Option<i64>,
    quote: Option<LatestQuote>,
    depth: Option<FiveLevelDepth>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MarketView {
    schema_version: u16,
    group_id: String,
    sequence: u64,
    trigger_event_id: String,
    as_of_received_at_ns_utc: i64,
    complete: bool,
    source_offset_gaps: u64,
    instruments: Box<[InstrumentView]>,
}

impl InstrumentView {
    #[must_use]
    pub fn metadata(&self) -> &InstrumentMetadata {
        &self.metadata
    }

    #[must_use]
    pub fn freshness(&self) -> &Freshness {
        &self.freshness
    }

    #[must_use]
    pub fn incomplete_reasons(&self) -> &[IncompleteReason] {
        &self.incomplete_reasons
    }

    #[must_use]
    pub fn last_event_id(&self) -> Option<&str> {
        self.last_event_id.as_deref()
    }

    #[must_use]
    pub fn last_source(&self) -> Option<&SourceCoordinate> {
        self.last_source.as_ref()
    }

    #[must_use]
    pub fn received_at_ns_utc(&self) -> Option<i64> {
        self.received_at_ns_utc
    }

    #[must_use]
    pub fn exchange_event_time_epoch_seconds_raw(&self) -> Option<i64> {
        self.exchange_event_time_epoch_seconds_raw
    }

    #[must_use]
    pub fn quote(&self) -> Option<&LatestQuote> {
        self.quote.as_ref()
    }

    #[must_use]
    pub fn depth(&self) -> Option<&FiveLevelDepth> {
        self.depth.as_ref()
    }
}

impl MarketView {
    #[must_use]
    pub fn schema_version(&self) -> u16 {
        self.schema_version
    }

    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub fn trigger_event_id(&self) -> &str {
        &self.trigger_event_id
    }

    #[must_use]
    pub fn as_of_received_at_ns_utc(&self) -> i64 {
        self.as_of_received_at_ns_utc
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    #[must_use]
    pub fn source_offset_gaps(&self) -> u64 {
        self.source_offset_gaps
    }

    #[must_use]
    pub fn instruments(&self) -> &[InstrumentView] {
        &self.instruments
    }
}

#[derive(Clone, Debug)]
struct InstrumentState {
    metadata: InstrumentMetadata,
    last_event_id: Option<String>,
    last_source: Option<SourceCoordinate>,
    received_at_ns_utc: Option<i64>,
    exchange_event_time_epoch_seconds_raw: Option<i64>,
    quote: Option<LatestQuote>,
    depth: Option<FiveLevelDepth>,
}

pub struct GoldMarketState {
    group_id: String,
    stale_after_ns: i64,
    instruments: BTreeMap<String, InstrumentState>,
    source_offsets: BTreeMap<SourcePartition, Option<i64>>,
    sequence: u64,
    as_of_received_at_ns_utc: Option<i64>,
    source_offset_gaps: u64,
}

impl GoldMarketState {
    pub fn new(config: MarketStateConfig) -> Result<Self> {
        config.validate()?;
        let instruments = config
            .instruments
            .into_iter()
            .map(|metadata| {
                (
                    metadata.security_id.clone(),
                    InstrumentState {
                        metadata,
                        last_event_id: None,
                        last_source: None,
                        received_at_ns_utc: None,
                        exchange_event_time_epoch_seconds_raw: None,
                        quote: None,
                        depth: None,
                    },
                )
            })
            .collect();
        let source_offsets = config
            .sources
            .into_iter()
            .map(|source| (source, None))
            .collect();
        Ok(Self {
            group_id: config.group_id,
            stale_after_ns: config.stale_after_ns,
            instruments,
            source_offsets,
            sequence: 0,
            as_of_received_at_ns_utc: None,
            source_offset_gaps: 0,
        })
    }

    pub fn apply(&mut self, event: &MarketEvent) -> Result<Arc<MarketView>> {
        let source = SourcePartition {
            topic: event.coordinate.topic.clone(),
            partition: event.coordinate.partition,
        };
        let previous_offset = self
            .source_offsets
            .get_mut(&source)
            .ok_or_else(|| anyhow::anyhow!("event came from unconfigured source {source:?}"))?;
        if let Some(previous) = *previous_offset {
            if event.coordinate.offset == previous {
                bail!("duplicate source offset {}", event.stable_id());
            }
            if event.coordinate.offset < previous {
                bail!(
                    "out-of-order source offset {} after {}/{}/{}",
                    event.stable_id(),
                    source.topic,
                    source.partition,
                    previous
                );
            }
            if event.coordinate.offset > previous.saturating_add(1) {
                self.source_offset_gaps = self.source_offset_gaps.saturating_add(1);
            }
        }
        let instrument = self
            .instruments
            .get_mut(&event.security_id)
            .ok_or_else(|| anyhow::anyhow!("unconfigured security ID {}", event.security_id))?;
        let (quote, depth) = decode_full_event(event)?;

        *previous_offset = Some(event.coordinate.offset);
        instrument.last_event_id = Some(event.stable_id());
        instrument.last_source = Some(event.coordinate.clone());
        instrument.received_at_ns_utc = Some(event.received_at_ns_utc);
        instrument.exchange_event_time_epoch_seconds_raw =
            event.exchange_event_time_epoch_seconds_raw;
        instrument.quote = Some(quote);
        instrument.depth = Some(depth);
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("market-view sequence overflow"))?;
        self.as_of_received_at_ns_utc = Some(
            self.as_of_received_at_ns_utc
                .map_or(event.received_at_ns_utc, |current| {
                    current.max(event.received_at_ns_utc)
                }),
        );
        Ok(Arc::new(self.snapshot(event.stable_id())?))
    }

    fn snapshot(&self, trigger_event_id: String) -> Result<MarketView> {
        let as_of = self
            .as_of_received_at_ns_utc
            .ok_or_else(|| anyhow::anyhow!("cannot snapshot state before its first event"))?;
        let mut complete = true;
        let instruments = self
            .instruments
            .values()
            .map(|state| {
                let freshness = match state.received_at_ns_utc {
                    None => Freshness::Missing,
                    Some(received_at) => {
                        let age_ns = as_of.saturating_sub(received_at);
                        if age_ns > self.stale_after_ns {
                            Freshness::Stale { age_ns }
                        } else {
                            Freshness::Fresh { age_ns }
                        }
                    }
                };
                let mut incomplete_reasons = Vec::new();
                if state.quote.is_none() {
                    incomplete_reasons.push(IncompleteReason::MissingQuote);
                }
                if state.depth.is_none() {
                    incomplete_reasons.push(IncompleteReason::MissingDepth);
                }
                if matches!(freshness, Freshness::Stale { .. }) {
                    incomplete_reasons.push(IncompleteReason::Stale);
                }
                if !incomplete_reasons.is_empty() {
                    complete = false;
                }
                InstrumentView {
                    metadata: state.metadata.clone(),
                    freshness,
                    incomplete_reasons,
                    last_event_id: state.last_event_id.clone(),
                    last_source: state.last_source.clone(),
                    received_at_ns_utc: state.received_at_ns_utc,
                    exchange_event_time_epoch_seconds_raw: state
                        .exchange_event_time_epoch_seconds_raw,
                    quote: state.quote.clone(),
                    depth: state.depth.clone(),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(MarketView {
            schema_version: MARKET_VIEW_SCHEMA_VERSION,
            group_id: self.group_id.clone(),
            sequence: self.sequence,
            trigger_event_id,
            as_of_received_at_ns_utc: as_of,
            complete,
            source_offset_gaps: self.source_offset_gaps,
            instruments,
        })
    }

    #[must_use]
    pub fn instrument_count(&self) -> usize {
        self.instruments.len()
    }

    #[must_use]
    pub fn source_count(&self) -> usize {
        self.source_offsets.len()
    }
}
