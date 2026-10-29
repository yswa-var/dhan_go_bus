use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use goldlab_market_core::{
    Freshness, GoldMarketState, InstrumentMetadata, MarketEvent, MarketStateConfig,
    SourceCoordinate, SourcePartition, load_resolved_contracts, spawn_market_state_actor,
};
use serde::Deserialize;

const SECURITY_IDS: [&str; 3] = ["581902", "582003", "495213"];

#[derive(Deserialize)]
struct BrokerEnvelope {
    topic: String,
    key: String,
    value: String,
    timestamp: i64,
    partition: i32,
    offset: i64,
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..")
}

fn fixture_events() -> Result<Vec<MarketEvent>> {
    let text = std::fs::read_to_string(
        repository_root()
            .join("fixtures")
            .join("q000")
            .join("live-records.jsonl"),
    )?;
    text.lines()
        .map(|line| {
            let envelope: BrokerEnvelope = serde_json::from_str(line)?;
            MarketEvent::from_parts(
                SourceCoordinate {
                    topic: envelope.topic,
                    partition: envelope.partition,
                    offset: envelope.offset,
                },
                Some(envelope.timestamp),
                envelope.key.as_bytes(),
                envelope.value.as_bytes(),
            )
        })
        .collect()
}

fn fixture_metadata() -> Result<Vec<InstrumentMetadata>> {
    let file =
        std::fs::File::open(repository_root().join("resolved-contracts-20261029T100331Z.csv"))?;
    let all = load_resolved_contracts(file)?;
    SECURITY_IDS
        .iter()
        .map(|security_id| {
            all.iter()
                .find(|metadata| metadata.security_id == *security_id)
                .cloned()
                .with_context(|| format!("metadata for {security_id}"))
        })
        .collect()
}

fn config(stale_after_ns: i64) -> Result<MarketStateConfig> {
    Ok(MarketStateConfig {
        group_id: "GOLD-fixture".to_owned(),
        stale_after_ns,
        instruments: fixture_metadata()?,
        sources: vec![SourcePartition {
            topic: "dhan.market.raw".to_owned(),
            partition: 0,
        }],
    })
}

#[test]
fn verified_full_packet_decodes_into_a_coherent_complete_view() -> Result<()> {
    let events = fixture_events()?;
    let mut state = GoldMarketState::new(config(1_000_000_000)?)?;
    let first = state.apply(&events[0])?;
    assert_eq!(first.sequence(), 1);
    assert!(!first.is_complete());
    assert_eq!(
        first
            .instruments()
            .iter()
            .filter(|instrument| matches!(instrument.freshness(), Freshness::Missing))
            .count(),
        2
    );

    let call = first
        .instruments()
        .iter()
        .find(|instrument| instrument.metadata().security_id == "581902")
        .context("call view")?;
    let quote = call.quote().context("call quote")?;
    assert_eq!(quote.last_price, 1060.0);
    assert_eq!(quote.last_trade_quantity, 2);
    assert_eq!(quote.average_price.to_bits(), 1_150_036_050);
    assert_eq!(quote.volume_raw, 33_959);
    assert_eq!(quote.total_sell_quantity, 740);
    assert_eq!(quote.total_buy_quantity, 13_593);
    assert_eq!(quote.open_interest, 2_675);
    let level = &call.depth().context("call depth")?.levels[0];
    assert_eq!(
        (
            level.bid_quantity,
            level.ask_quantity,
            level.bid_orders,
            level.ask_orders,
            level.bid_price,
            level.ask_price,
        ),
        (1, 5, 1, 4, 1056.0, 1058.0)
    );

    let second = state.apply(&events[1])?;
    assert_eq!(second.sequence(), 2);
    assert!(!second.is_complete());
    let third = state.apply(&events[2])?;
    assert_eq!(third.sequence(), 3);
    assert!(third.is_complete());
    assert_eq!(third.trigger_event_id(), "dhan.market.raw/0/71181");
    assert_eq!(third.source_offset_gaps(), 2);
    assert!(third.instruments().iter().all(|instrument| {
        instrument.incomplete_reasons().is_empty()
            && matches!(instrument.freshness(), Freshness::Fresh { .. })
            && instrument.quote().is_some()
            && instrument.depth().is_some()
            && instrument
                .depth()
                .is_some_and(|depth| depth.levels.len() == 5)
    }));
    Ok(())
}

#[test]
fn stale_and_missing_instruments_are_explicit_and_receive_clock_never_regresses() -> Result<()> {
    let mut events = fixture_events()?;
    let mut state = GoldMarketState::new(config(1)?)?;
    let first = state.apply(&events[0])?;
    assert!(!first.is_complete());

    events[1].received_at_ns_utc = events[0].received_at_ns_utc - 1_000;
    let regressing = state.apply(&events[1])?;
    assert_eq!(
        regressing.as_of_received_at_ns_utc(),
        first.as_of_received_at_ns_utc()
    );
    let second = state.apply(&events[2])?;
    assert!(!second.is_complete());
    assert!(second.instruments().iter().any(|instrument| {
        instrument.metadata().security_id == "581902"
            && matches!(instrument.freshness(), Freshness::Stale { .. })
    }));
    Ok(())
}

#[test]
fn duplicate_out_of_order_unknown_and_unsupported_events_fail_without_advancing() -> Result<()> {
    let events = fixture_events()?;
    let mut state = GoldMarketState::new(config(1_000_000_000)?)?;
    assert_eq!(state.apply(&events[0])?.sequence(), 1);

    assert!(
        state
            .apply(&events[0])
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
    let mut out_of_order = events[0].clone();
    out_of_order.coordinate.offset -= 1;
    assert!(
        state
            .apply(&out_of_order)
            .unwrap_err()
            .to_string()
            .contains("out-of-order")
    );
    let mut unknown = events[1].clone();
    unknown.security_id = "999999".to_owned();
    assert!(
        state
            .apply(&unknown)
            .unwrap_err()
            .to_string()
            .contains("unconfigured security ID")
    );
    let mut unsupported = events[1].clone();
    unsupported.packet_type = 4;
    unsupported.raw_packet[0] = 4;
    assert!(
        state
            .apply(&unsupported)
            .unwrap_err()
            .to_string()
            .contains("unsupported packet type")
    );

    assert_eq!(state.apply(&events[1])?.sequence(), 2);
    Ok(())
}

#[test]
fn state_size_is_fixed_during_long_replay() -> Result<()> {
    let mut event = fixture_events()?.remove(0);
    let metadata = fixture_metadata()?
        .into_iter()
        .filter(|item| item.security_id == event.security_id)
        .collect();
    let mut state = GoldMarketState::new(MarketStateConfig {
        group_id: "bounded".to_owned(),
        stale_after_ns: 1_000_000_000,
        instruments: metadata,
        sources: vec![SourcePartition {
            topic: event.coordinate.topic.clone(),
            partition: event.coordinate.partition,
        }],
    })?;
    for offset in 0..10_000 {
        event.coordinate.offset = offset;
        event.received_at_ns_utc = event.received_at_ns_utc.saturating_add(1);
        let snapshot = state.apply(&event)?;
        assert_eq!(snapshot.instruments().len(), 1);
    }
    assert_eq!(state.instrument_count(), 1);
    assert_eq!(state.source_count(), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn actor_has_one_owner_and_publishes_one_immutable_snapshot_per_event() -> Result<()> {
    let events = fixture_events()?;
    let actor = spawn_market_state_actor(config(1_000_000_000)?, 1, 1)?;
    let mut snapshots = actor.snapshots;
    let sender = actor.events;
    let task = actor.task;
    for (index, event) in events.into_iter().enumerate() {
        sender.send(event).await?;
        let snapshot = snapshots.recv().await.context("snapshot channel closed")?;
        assert_eq!(snapshot.sequence(), u64::try_from(index + 1)?);
    }
    drop(sender);
    let stats = task.await??;
    assert_eq!(stats.events_applied, 3);
    assert_eq!(stats.snapshots_published, 3);
    assert!(snapshots.recv().await.is_none());
    Ok(())
}
