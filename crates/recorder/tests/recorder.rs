use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::{Array, BinaryArray, Int32Array, Int64Array, StringArray};
use goldlab_recorder::batch::{BatchBuffer, BatchLimits};
use goldlab_recorder::record::{SourceCoordinate, SourceRecord};
use goldlab_recorder::storage::{CrashPoint, Storage};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
use tempfile::TempDir;

const FIXTURE_SHA256: &str = "9f613acd730740824b00ec7d9e7f4cecea71d7f4a54b144dbdb61f01caae3a03";

#[derive(Deserialize)]
struct BrokerEnvelope {
    topic: String,
    key: String,
    value: String,
    timestamp: i64,
    partition: i32,
    offset: i64,
}

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("q000")
        .join("live-records.jsonl")
}

fn fixture_records() -> Result<Vec<SourceRecord>> {
    let text = std::fs::read_to_string(fixture_path()).context("read Q000 fixture")?;
    text.lines()
        .map(|line| {
            let envelope: BrokerEnvelope = serde_json::from_str(line)?;
            SourceRecord::from_parts(
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

fn fixture_storage(directory: &TempDir) -> Result<Storage> {
    let (storage, _) = Storage::open_with_metadata(
        directory.path(),
        "fixture-revision",
        r#"{"source":"q000"}"#,
        Some(format!("sha256:{FIXTURE_SHA256}")),
    )?;
    Ok(storage)
}

#[test]
fn q000_fixture_round_trips_exact_coordinates_and_bytes() -> Result<()> {
    let records = fixture_records()?;
    assert_eq!(records.len(), 3);
    let expected = records.clone();
    let directory = TempDir::new()?;
    let mut storage = fixture_storage(&directory)?;

    let result = storage.finalize(records)?;
    assert_eq!(result.rows, 3);
    assert_eq!(result.files, 1);
    assert_eq!(
        result.ranges.len(),
        3,
        "non-contiguous offsets stay explicit"
    );
    assert_eq!(storage.manifest().files.len(), 1);
    let entry = &storage.manifest().files[0];
    assert_eq!(entry.row_count, 3);
    let expected_provenance = format!("sha256:{FIXTURE_SHA256}");
    assert_eq!(
        entry.fixture_provenance.as_deref(),
        Some(expected_provenance.as_str())
    );

    let parquet_path = directory.path().join(&entry.path);
    let file = File::open(parquet_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut actual_coordinates = Vec::new();
    let mut actual_raw_packets = Vec::new();
    for batch in reader {
        let batch = batch?;
        let topics = batch
            .column_by_name("source_topic")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .context("source_topic")?;
        let partitions = batch
            .column_by_name("source_partition")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .context("source_partition")?;
        let offsets = batch
            .column_by_name("source_offset")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .context("source_offset")?;
        let raw_packets = batch
            .column_by_name("raw_packet")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .context("raw_packet")?;
        for row in 0..batch.num_rows() {
            actual_coordinates.push((
                topics.value(row).to_owned(),
                partitions.value(row),
                offsets.value(row),
            ));
            actual_raw_packets.push(raw_packets.value(row).to_vec());
        }
    }
    assert_eq!(
        actual_coordinates,
        expected
            .iter()
            .map(|record| (
                record.coordinate.topic.clone(),
                record.coordinate.partition,
                record.coordinate.offset
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        actual_raw_packets,
        expected
            .iter()
            .map(|record| record.raw_packet.clone())
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn malformed_event_is_rejected_before_recording() -> Result<()> {
    let mut envelope: BrokerEnvelope = serde_json::from_str(
        std::fs::read_to_string(fixture_path())?
            .lines()
            .next()
            .context("fixture is empty")?,
    )?;
    envelope.key = "MCX:wrong".to_owned();
    let error = SourceRecord::from_parts(
        SourceCoordinate {
            topic: envelope.topic,
            partition: envelope.partition,
            offset: envelope.offset,
        },
        Some(envelope.timestamp),
        envelope.key.as_bytes(),
        envelope.value.as_bytes(),
    )
    .expect_err("mismatched key must fail");
    assert!(error.to_string().contains("does not match event identity"));
    Ok(())
}

#[test]
fn crash_before_rename_removes_temporary_file_and_replays() -> Result<()> {
    let records = fixture_records()?;
    let directory = TempDir::new()?;
    let mut storage = fixture_storage(&directory)?;
    let error = storage
        .finalize_with_crash_point(records.clone(), CrashPoint::BeforeParquetRename)
        .expect_err("injected crash must fail");
    assert!(error.to_string().contains("before Parquet rename"));
    drop(storage);

    let (mut recovered, report) = Storage::open_with_metadata(
        directory.path(),
        "fixture-revision",
        r#"{"source":"q000"}"#,
        Some(format!("sha256:{FIXTURE_SHA256}")),
    )?;
    assert_eq!(report.removed_temporary_files, 1);
    assert_eq!(report.adopted_parquet_files, 0);
    assert!(recovered.manifest().files.is_empty());
    assert_eq!(recovered.finalize(records)?.rows, 3);
    Ok(())
}

#[test]
fn crash_after_rename_adopts_file_and_deduplicates_replay() -> Result<()> {
    let records = fixture_records()?;
    let directory = TempDir::new()?;
    let mut storage = fixture_storage(&directory)?;
    let error = storage
        .finalize_with_crash_point(
            records.clone(),
            CrashPoint::AfterParquetRenameBeforeManifest,
        )
        .expect_err("injected crash must fail");
    assert!(error.to_string().contains("before manifest"));
    drop(storage);

    let (mut recovered, report) = Storage::open_with_metadata(
        directory.path(),
        "fixture-revision",
        r#"{"source":"q000"}"#,
        Some(format!("sha256:{FIXTURE_SHA256}")),
    )?;
    assert_eq!(report.removed_temporary_files, 0);
    assert_eq!(report.adopted_parquet_files, 1);
    assert_eq!(recovered.manifest().files.len(), 1);
    for record in &records {
        assert!(recovered.is_finalized(&record.coordinate));
    }
    let duplicate = recovered.finalize(records)?;
    assert_eq!(duplicate.rows, 0);
    assert_eq!(duplicate.files, 0);
    assert_eq!(recovered.manifest().files.len(), 1);
    Ok(())
}

#[test]
fn durable_manifest_deduplicates_replay_before_kafka_commit() -> Result<()> {
    let records = fixture_records()?;
    let directory = TempDir::new()?;
    let mut storage = fixture_storage(&directory)?;
    assert_eq!(storage.finalize(records.clone())?.rows, 3);
    drop(storage);

    let mut restarted = fixture_storage(&directory)?;
    let replay = restarted.finalize(records)?;
    assert_eq!(replay.rows, 0);
    assert_eq!(replay.files, 0);
    assert_eq!(restarted.manifest().files.len(), 1);
    Ok(())
}

#[test]
fn duplicate_coordinate_inside_batch_is_rejected() -> Result<()> {
    let mut records = fixture_records()?;
    records.push(records[0].clone());
    let directory = TempDir::new()?;
    let mut storage = fixture_storage(&directory)?;
    let error = storage
        .finalize(records)
        .expect_err("duplicate source coordinate must fail");
    assert!(error.to_string().contains("duplicate source coordinate"));
    assert!(storage.manifest().files.is_empty());
    Ok(())
}

#[test]
fn batch_limits_bound_record_count() -> Result<()> {
    let mut records = fixture_records()?.into_iter();
    let limits = BatchLimits {
        max_records: 2,
        max_bytes: usize::MAX,
        max_age: Duration::from_secs(60),
    }
    .validate()?;
    let mut batch = BatchBuffer::new(limits);
    batch.push(records.next().context("first record")?);
    assert!(!batch.should_rotate());
    batch.push(records.next().context("second record")?);
    assert!(batch.should_rotate());
    assert_eq!(batch.take().len(), 2);
    assert!(batch.is_empty());
    Ok(())
}
