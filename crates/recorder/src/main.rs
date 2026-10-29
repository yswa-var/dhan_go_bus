use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use goldlab_recorder::batch::{BatchBuffer, BatchLimits};
use goldlab_recorder::manifest::OffsetRange;
use goldlab_recorder::record::{SourceCoordinate, SourceRecord};
use goldlab_recorder::storage::{FinalizeResult, Storage};
use rdkafka::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Message, Timestamp};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(about = "Durably record GOLDLAB Kafka events as Parquet")]
struct Args {
    #[arg(long, env = "BROKER", default_value = "localhost:19092")]
    broker: String,
    #[arg(long, env = "TOPIC", default_value = "dhan.market.raw")]
    topic: String,
    #[arg(long, env = "RECORDER_GROUP", default_value = "goldlab-recorder-v1")]
    group: String,
    #[arg(long, env = "RECORDER_OUTPUT", default_value = "recordings")]
    output: PathBuf,
    #[arg(long, env = "RECORDER_MAX_RECORDS", default_value_t = 10_000)]
    max_records: usize,
    #[arg(long, env = "RECORDER_MAX_BYTES", default_value_t = 67_108_864)]
    max_bytes: usize,
    #[arg(long, env = "RECORDER_ROTATE_SECONDS", default_value_t = 60)]
    rotate_seconds: u64,
    #[arg(long, env = "GOLDLAB_CODE_REVISION", default_value = "unknown")]
    code_revision: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    run(args).await
}

async fn run(args: Args) -> Result<()> {
    let limits = BatchLimits {
        max_records: args.max_records,
        max_bytes: args.max_bytes,
        max_age: Duration::from_secs(args.rotate_seconds),
    }
    .validate()?;
    let configuration_json = serde_json::to_string(&serde_json::json!({
        "broker": &args.broker,
        "topic": &args.topic,
        "group": &args.group,
        "max_records": limits.max_records,
        "max_bytes": limits.max_bytes,
        "rotate_seconds": limits.max_age.as_secs(),
    }))?;
    let (mut storage, recovery) =
        Storage::open_with_metadata(&args.output, &args.code_revision, configuration_json, None)?;
    info!(
        removed_temporary_files = recovery.removed_temporary_files,
        adopted_parquet_files = recovery.adopted_parquet_files,
        "storage recovery complete"
    );

    let consumer: Arc<StreamConsumer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", &args.broker)
            .set("group.id", &args.group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed")
            .set("queued.max.messages.kbytes", "65536")
            .set("max.poll.interval.ms", "300000")
            .create()
            .context("create Kafka consumer")?,
    );
    consumer
        .subscribe(&[&args.topic])
        .with_context(|| format!("subscribe to {}", args.topic))?;
    info!(
        broker = %args.broker,
        topic = %args.topic,
        group = %args.group,
        output = %args.output.display(),
        max_records = limits.max_records,
        max_bytes = limits.max_bytes,
        rotate_seconds = limits.max_age.as_secs(),
        "recorder started"
    );

    let mut batch = BatchBuffer::new(limits);
    let mut rotation_tick = tokio::time::interval(limits.max_age.min(Duration::from_secs(1)));
    rotation_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut received = 0_u64;
    let mut deduplicated = 0_u64;
    let mut written = 0_u64;

    loop {
        tokio::select! {
            signal = shutdown_signal() => {
                signal?;
                info!("shutdown requested");
                break;
            }
            _ = rotation_tick.tick() => {
                if batch.age_expired() {
                    let (returned_storage, result) = flush(storage, &mut batch, Arc::clone(&consumer)).await?;
                    storage = returned_storage;
                    written += u64::try_from(result.rows).unwrap_or(u64::MAX);
                    log_finalize(&result, written, received, deduplicated);
                }
            }
            message = consumer.recv() => {
                let message = message.context("receive Kafka record")?;
                received = received.saturating_add(1);
                let coordinate = SourceCoordinate {
                    topic: message.topic().to_owned(),
                    partition: message.partition(),
                    offset: message.offset(),
                };
                let payload = message.payload().context("Kafka record has no value")?;
                let key = message.key().context("Kafka record has no key")?;
                let kafka_timestamp_ms = match message.timestamp() {
                    Timestamp::NotAvailable => None,
                    Timestamp::CreateTime(value) | Timestamp::LogAppendTime(value) => Some(value),
                };
                let record = SourceRecord::from_parts(coordinate.clone(), kafka_timestamp_ms, key, payload)
                    .with_context(|| format!("validate {}/{}/{}", coordinate.topic, coordinate.partition, coordinate.offset))?;

                if storage.is_finalized(&coordinate) {
                    if !batch.is_empty() {
                        let (returned_storage, result) = flush(storage, &mut batch, Arc::clone(&consumer)).await?;
                        storage = returned_storage;
                        written += u64::try_from(result.rows).unwrap_or(u64::MAX);
                        log_finalize(&result, written, received, deduplicated);
                    }
                    commit_ranges(
                        Arc::clone(&consumer),
                        vec![OffsetRange {
                            topic: coordinate.topic,
                            partition: coordinate.partition,
                            first_offset: coordinate.offset,
                            last_offset: coordinate.offset,
                        }],
                    ).await?;
                    deduplicated = deduplicated.saturating_add(1);
                    continue;
                }

                batch.push(record);
                if batch.should_rotate() {
                    let (returned_storage, result) = flush(storage, &mut batch, Arc::clone(&consumer)).await?;
                    storage = returned_storage;
                    written += u64::try_from(result.rows).unwrap_or(u64::MAX);
                    log_finalize(&result, written, received, deduplicated);
                }
            }
        }
    }

    if !batch.is_empty() {
        let (_storage, result) = flush(storage, &mut batch, Arc::clone(&consumer)).await?;
        written += u64::try_from(result.rows).unwrap_or(u64::MAX);
        log_finalize(&result, written, received, deduplicated);
    }
    info!(received, written, deduplicated, "recorder stopped cleanly");
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("listen for Ctrl+C")?,
        _ = terminate.recv() => {},
    }
    Ok(())
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await.context("listen for Ctrl+C")
}

async fn flush(
    storage: Storage,
    batch: &mut BatchBuffer,
    consumer: Arc<StreamConsumer>,
) -> Result<(Storage, FinalizeResult)> {
    let records = batch.take();
    if records.is_empty() {
        return Ok((storage, FinalizeResult::default()));
    }
    let (returned_storage, result) = tokio::task::spawn_blocking(move || {
        let mut storage = storage;
        let result = storage.finalize(records);
        (storage, result)
    })
    .await
    .context("join Parquet finalization task")?;
    let result = result?;
    commit_ranges(consumer, result.ranges.clone()).await?;
    Ok((returned_storage, result))
}

async fn commit_ranges(consumer: Arc<StreamConsumer>, ranges: Vec<OffsetRange>) -> Result<()> {
    let mut next_offsets: BTreeMap<(String, i32), i64> = BTreeMap::new();
    for range in ranges {
        let next = range
            .last_offset
            .checked_add(1)
            .context("Kafka offset overflow")?;
        next_offsets
            .entry((range.topic, range.partition))
            .and_modify(|current| *current = (*current).max(next))
            .or_insert(next);
    }
    if next_offsets.is_empty() {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || {
        let mut offsets = TopicPartitionList::new();
        for ((topic, partition), next) in next_offsets {
            offsets
                .add_partition_offset(&topic, partition, Offset::Offset(next))
                .with_context(|| format!("prepare commit for {topic}/{partition}/{next}"))?;
        }
        consumer
            .commit(&offsets, CommitMode::Sync)
            .context("commit durable Kafka offsets")
    })
    .await
    .context("join Kafka commit task")??;
    Ok(())
}

fn log_finalize(result: &FinalizeResult, written: u64, received: u64, deduplicated: u64) {
    if result.rows == 0 {
        warn!(
            received,
            written, deduplicated, "flush contained no new rows"
        );
    } else {
        info!(
            rows = result.rows,
            files = result.files,
            received,
            written,
            deduplicated,
            "durable Parquet batch committed"
        );
    }
}
