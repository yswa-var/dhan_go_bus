use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use goldlab_market_core::{
    Freshness, GoldMarketState, MARKET_VIEW_SCHEMA_VERSION, MarketStateConfig, MarketView,
    SourcePartition, load_resolved_contracts,
};
use goldlab_replay::RecordingReader;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Parser)]
#[command(about = "Build deterministic GOLD market views from a Q001 recording")]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    contracts: PathBuf,
    #[arg(long)]
    group_id: String,
    #[arg(long)]
    stale_after_ms: u64,
    #[arg(long, default_value = "dhan.market.raw")]
    source_topic: String,
    #[arg(long, value_delimiter = ',', default_value = "0")]
    source_partitions: Vec<i32>,
    #[arg(long, default_value_t = 1024)]
    batch_records: usize,
    #[arg(long)]
    last_view_output: Option<PathBuf>,
}

#[derive(Serialize)]
struct StateReplaySummary {
    market_view_schema_version: u16,
    group_id: String,
    manifest_sha256: String,
    configuration_sha256: String,
    configured_instruments: usize,
    configured_sources: usize,
    event_count: u64,
    final_sequence: u64,
    final_complete: bool,
    final_fresh_instruments: usize,
    final_stale_instruments: usize,
    final_missing_instruments: usize,
    source_offset_gaps: u64,
    received_time_regressions: u64,
    snapshot_stream_sha256: String,
    final_view_sha256: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let summary = tokio::task::spawn_blocking(move || run(args))
        .await
        .context("join market-state replay worker")??;
    serde_json::to_writer_pretty(io::stdout().lock(), &summary)
        .context("write market-state replay summary")?;
    println!();
    Ok(())
}

fn run(args: Args) -> Result<StateReplaySummary> {
    ensure!(args.stale_after_ms > 0, "stale_after_ms must be positive");
    let stale_after_ns = i64::try_from(args.stale_after_ms)
        .ok()
        .and_then(|milliseconds| milliseconds.checked_mul(1_000_000))
        .context("stale_after_ms is too large")?;
    let contract_file = File::open(&args.contracts)
        .with_context(|| format!("open contracts file {}", args.contracts.display()))?;
    let instruments = load_resolved_contracts(contract_file)?;
    let mut sources = args
        .source_partitions
        .iter()
        .map(|partition| SourcePartition {
            topic: args.source_topic.clone(),
            partition: *partition,
        })
        .collect::<Vec<_>>();
    sources.sort();
    let configured_instruments = instruments.len();
    let configured_sources = sources.len();
    let config = MarketStateConfig {
        group_id: args.group_id.clone(),
        stale_after_ns,
        instruments,
        sources,
    };
    let config_bytes = serde_json::to_vec(&config).context("serialize market-state config")?;
    let configuration_sha256 = hex::encode(Sha256::digest(&config_bytes));
    let mut state = GoldMarketState::new(config)?;
    let reader = RecordingReader::open(&args.input, args.batch_records)?;
    let manifest_sha256 = reader.manifest_sha256().to_owned();
    let mut snapshot_digest = Sha256::new();
    snapshot_digest.update(b"goldlab-market-view-stream-v1");
    snapshot_digest.update(&config_bytes);
    let mut final_view = None;
    let mut previous_received_at = None;
    let mut received_time_regressions = 0_u64;
    let stats = reader.visit(|event| {
        if previous_received_at.is_some_and(|previous| event.received_at_ns_utc < previous) {
            received_time_regressions = received_time_regressions.saturating_add(1);
        }
        previous_received_at = Some(event.received_at_ns_utc);
        let view = state.apply(&event)?;
        update_snapshot_digest(&mut snapshot_digest, view.as_ref());
        final_view = Some(view);
        Ok(())
    })?;
    let final_view = final_view.context("recording produced no market views")?;
    let final_bytes = serde_json::to_vec(final_view.as_ref()).context("serialize final view")?;
    if let Some(path) = args.last_view_output {
        let file = File::create(&path)
            .with_context(|| format!("create last-view output {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, final_view.as_ref())
            .context("write final market view")?;
        writer
            .write_all(b"\n")
            .context("write final-view newline")?;
        writer.flush().context("flush final-view output")?;
        writer
            .get_ref()
            .sync_all()
            .context("sync final-view output")?;
    }
    let mut fresh = 0_usize;
    let mut stale = 0_usize;
    let mut missing = 0_usize;
    for instrument in final_view.instruments() {
        match instrument.freshness() {
            Freshness::Fresh { .. } => fresh += 1,
            Freshness::Stale { .. } => stale += 1,
            Freshness::Missing => missing += 1,
        }
    }
    Ok(StateReplaySummary {
        market_view_schema_version: MARKET_VIEW_SCHEMA_VERSION,
        group_id: args.group_id,
        manifest_sha256,
        configuration_sha256,
        configured_instruments,
        configured_sources,
        event_count: stats.events,
        final_sequence: final_view.sequence(),
        final_complete: final_view.is_complete(),
        final_fresh_instruments: fresh,
        final_stale_instruments: stale,
        final_missing_instruments: missing,
        source_offset_gaps: final_view.source_offset_gaps(),
        received_time_regressions,
        snapshot_stream_sha256: hex::encode(snapshot_digest.finalize()),
        final_view_sha256: hex::encode(Sha256::digest(final_bytes)),
    })
}

fn update_snapshot_digest(digest: &mut Sha256, view: &MarketView) {
    digest.update(view.schema_version().to_be_bytes());
    digest_bytes(digest, view.group_id().as_bytes());
    digest.update(view.sequence().to_be_bytes());
    digest_bytes(digest, view.trigger_event_id().as_bytes());
    digest.update(view.as_of_received_at_ns_utc().to_be_bytes());
    digest.update([u8::from(view.is_complete())]);
    digest.update(view.source_offset_gaps().to_be_bytes());
    digest.update(
        u64::try_from(view.instruments().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for instrument in view.instruments() {
        digest_bytes(digest, instrument.metadata().security_id.as_bytes());
        match instrument.freshness() {
            Freshness::Missing => digest.update([0]),
            Freshness::Fresh { age_ns } => {
                digest.update([1]);
                digest.update(age_ns.to_be_bytes());
            }
            Freshness::Stale { age_ns } => {
                digest.update([2]);
                digest.update(age_ns.to_be_bytes());
            }
        }
        optional_bytes(digest, instrument.last_event_id().map(str::as_bytes));
        if let Some(source) = instrument.last_source() {
            digest.update([1]);
            digest_bytes(digest, source.topic.as_bytes());
            digest.update(source.partition.to_be_bytes());
            digest.update(source.offset.to_be_bytes());
        } else {
            digest.update([0]);
        }
        optional_i64(digest, instrument.received_at_ns_utc());
        optional_i64(digest, instrument.exchange_event_time_epoch_seconds_raw());
        if let Some(quote) = instrument.quote() {
            digest.update([1]);
            for value in [
                quote.last_price,
                quote.average_price,
                quote.open,
                quote.close,
                quote.high,
                quote.low,
            ] {
                digest.update(value.to_bits().to_be_bytes());
            }
            digest.update(quote.last_trade_quantity.to_be_bytes());
            digest.update(quote.volume_raw.to_be_bytes());
            digest.update(quote.total_sell_quantity.to_be_bytes());
            digest.update(quote.total_buy_quantity.to_be_bytes());
            digest.update(quote.open_interest.to_be_bytes());
        } else {
            digest.update([0]);
        }
        if let Some(depth) = instrument.depth() {
            digest.update([1]);
            for level in &depth.levels {
                digest.update(level.bid_quantity.to_be_bytes());
                digest.update(level.ask_quantity.to_be_bytes());
                digest.update(level.bid_orders.to_be_bytes());
                digest.update(level.ask_orders.to_be_bytes());
                digest.update(level.bid_price.to_bits().to_be_bytes());
                digest.update(level.ask_price.to_bits().to_be_bytes());
            }
        } else {
            digest.update([0]);
        }
    }
}

fn optional_bytes(digest: &mut Sha256, value: Option<&[u8]>) {
    if let Some(value) = value {
        digest.update([1]);
        digest_bytes(digest, value);
    } else {
        digest.update([0]);
    }
}

fn optional_i64(digest: &mut Sha256, value: Option<i64>) {
    if let Some(value) = value {
        digest.update([1]);
        digest.update(value.to_be_bytes());
    } else {
        digest.update([0]);
    }
}

fn digest_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}
