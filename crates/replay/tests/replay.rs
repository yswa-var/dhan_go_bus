use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use goldlab_recorder::record::{SourceCoordinate, SourceRecord};
use goldlab_recorder::storage::Storage;
use goldlab_replay::{ORDERING_POLICY, RecordingReader, ReplayEngine, ReplaySpeed};
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

fn write_fixture(directory: &TempDir, records: Vec<SourceRecord>) -> Result<()> {
    let (mut storage, recovery) = Storage::open_with_metadata(
        directory.path(),
        "fixture-revision",
        r#"{"source":"q000","replay_order":"topic_partition_offset_v1"}"#,
        Some(format!("sha256:{FIXTURE_SHA256}")),
    )?;
    assert_eq!(recovery.adopted_parquet_files, 0);
    assert_eq!(recovery.removed_temporary_files, 0);
    storage.finalize(records)?;
    Ok(())
}

fn replay(directory: &TempDir) -> Result<(Vec<u8>, goldlab_replay::ReplaySummary)> {
    let reader = RecordingReader::open(directory.path(), 2)?;
    let mut rows = Vec::new();
    let summary = ReplayEngine::new(ReplaySpeed::Max).run(reader, &mut rows)?;
    Ok((rows, summary))
}

#[test]
fn q000_fixture_replay_is_byte_for_byte_deterministic() -> Result<()> {
    let directory = TempDir::new()?;
    write_fixture(&directory, fixture_records()?)?;

    let (first_rows, first_summary) = replay(&directory)?;
    let (second_rows, second_summary) = replay(&directory)?;
    let wall_clock_reader = RecordingReader::open(directory.path(), 2)?;
    let mut wall_clock_rows = Vec::new();
    let wall_clock_summary =
        ReplayEngine::new(ReplaySpeed::WallClock).run(wall_clock_reader, &mut wall_clock_rows)?;

    assert_eq!(first_rows, second_rows);
    assert_eq!(first_summary, second_summary);
    assert_eq!(first_rows, wall_clock_rows);
    assert_eq!(
        first_summary.checksum_sha256,
        wall_clock_summary.checksum_sha256
    );
    assert_eq!(wall_clock_summary.speed, ReplaySpeed::WallClock);
    assert_eq!(first_summary.event_count, 3);
    assert_eq!(first_summary.partitions, 1);
    assert_eq!(first_summary.offset_gaps, 2);
    assert_eq!(first_summary.missing_offsets, 91);
    assert_eq!(first_summary.received_time_regressions, 0);
    assert_eq!(first_summary.ordering_policy, ORDERING_POLICY);
    assert_eq!(
        first_summary.checksum_sha256,
        "d2ccd8036a82cd96e283502956b8d3d0e25f696302d316f8557d2185959025a0"
    );

    let rows = std::str::from_utf8(&first_rows)?
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let event_ids = rows
        .iter()
        .map(|row| row["event_id"].as_str().context("event_id"))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        event_ids,
        [
            "dhan.market.raw/0/71088",
            "dhan.market.raw/0/71156",
            "dhan.market.raw/0/71181"
        ]
    );
    Ok(())
}

#[test]
fn cross_partition_merge_is_lexicographic_not_time_based() -> Result<()> {
    let fixture = fixture_records()?;
    let mut partition_one_earlier_time = fixture[0].clone();
    partition_one_earlier_time.coordinate.partition = 1;
    partition_one_earlier_time.coordinate.offset = 1;
    let mut partition_zero_later_time = fixture[1].clone();
    partition_zero_later_time.coordinate.partition = 0;
    partition_zero_later_time.coordinate.offset = 2;

    let directory = TempDir::new()?;
    write_fixture(
        &directory,
        vec![partition_one_earlier_time, partition_zero_later_time],
    )?;
    let reader = RecordingReader::open(directory.path(), 1)?;
    let mut coordinates = Vec::new();
    let stats = reader.visit(|event| {
        coordinates.push((event.coordinate.partition, event.coordinate.offset));
        Ok(())
    })?;

    assert_eq!(coordinates, [(0, 2), (1, 1)]);
    assert_eq!(stats.partitions, 2);
    Ok(())
}

#[test]
fn changed_manifest_checksum_is_rejected_before_rows_are_emitted() -> Result<()> {
    let directory = TempDir::new()?;
    write_fixture(&directory, fixture_records()?)?;
    let manifest_path = directory.path().join("manifest.json");
    let mut manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
    manifest["files"][0]["sha256"] = serde_json::Value::String("0".repeat(64));
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    let error = RecordingReader::open(directory.path(), 2)
        .err()
        .context("changed checksum must fail")?;
    assert!(error.to_string().contains("checksum mismatch"));
    Ok(())
}

#[test]
fn unmanifested_parquet_file_is_rejected() -> Result<()> {
    let directory = TempDir::new()?;
    write_fixture(&directory, fixture_records()?)?;
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("manifest.json"))?)?;
    let recorded_path = manifest["files"][0]["path"]
        .as_str()
        .context("manifest file path")?;
    std::fs::copy(
        directory.path().join(recorded_path),
        directory.path().join("unmanifested.parquet"),
    )?;

    let error = RecordingReader::open(directory.path(), 2)
        .err()
        .context("unmanifested Parquet must fail")?;
    assert!(error.to_string().contains("unmanifested or missing"));
    Ok(())
}
