use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray, UInt8Array,
    UInt16Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};

use crate::manifest::{
    FileEntry, Manifest, OffsetRange, relative_path, resolve_manifest_path, sync_directory,
};
use crate::record::{PARQUET_SCHEMA_VERSION, SourceCoordinate, SourceRecord};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrashPoint {
    Never,
    BeforeParquetRename,
    AfterParquetRenameBeforeManifest,
}

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub removed_temporary_files: usize,
    pub adopted_parquet_files: usize,
}

#[derive(Debug, Default)]
pub struct FinalizeResult {
    pub rows: usize,
    pub files: usize,
    pub ranges: Vec<OffsetRange>,
}

pub struct Storage {
    root: PathBuf,
    code_revision: String,
    configuration_json: String,
    fixture_provenance: Option<String>,
    manifest: Manifest,
}

impl Storage {
    pub fn open(
        root: impl Into<PathBuf>,
        code_revision: impl Into<String>,
    ) -> Result<(Self, RecoveryReport)> {
        Self::open_with_metadata(root, code_revision, "{}", None)
    }

    pub fn open_with_metadata(
        root: impl Into<PathBuf>,
        code_revision: impl Into<String>,
        configuration_json: impl Into<String>,
        fixture_provenance: Option<String>,
    ) -> Result<(Self, RecoveryReport)> {
        let root = root.into();
        fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
        let code_revision = code_revision.into();
        let configuration_json = configuration_json.into();
        let _: serde_json::Value = serde_json::from_str(&configuration_json)
            .context("configuration_json must contain valid JSON")?;
        let mut manifest = Manifest::load(&root)?;
        let report = recover_files(
            &root,
            &code_revision,
            &configuration_json,
            fixture_provenance.as_deref(),
            &mut manifest,
        )?;
        if report.adopted_parquet_files > 0 {
            manifest.persist(&root)?;
        }
        Ok((
            Self {
                root,
                code_revision,
                configuration_json,
                fixture_provenance,
                manifest,
            },
            report,
        ))
    }

    #[must_use]
    pub fn is_finalized(&self, coordinate: &SourceCoordinate) -> bool {
        self.manifest.contains(coordinate)
    }

    #[must_use]
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn finalize(&mut self, records: Vec<SourceRecord>) -> Result<FinalizeResult> {
        self.finalize_with_crash_point(records, CrashPoint::Never)
    }

    pub fn finalize_with_crash_point(
        &mut self,
        records: Vec<SourceRecord>,
        crash_point: CrashPoint,
    ) -> Result<FinalizeResult> {
        if records.is_empty() {
            return Ok(FinalizeResult::default());
        }
        let mut grouped: BTreeMap<(String, i32), Vec<SourceRecord>> = BTreeMap::new();
        for record in records {
            if self.is_finalized(&record.coordinate) {
                continue;
            }
            grouped
                .entry((record.coordinate.topic.clone(), record.coordinate.partition))
                .or_default()
                .push(record);
        }

        let mut result = FinalizeResult::default();
        for ((topic, partition), mut partition_records) in grouped {
            partition_records.sort_by_key(|record| record.coordinate.offset);
            ensure_unique_and_ordered(&partition_records)?;
            let ranges = offset_ranges(&partition_records);
            let entry = self.write_partition_file(
                &topic,
                partition,
                &partition_records,
                &ranges,
                crash_point,
            )?;
            result.rows += entry.row_count;
            result.files += 1;
            result.ranges.extend(entry.ranges.clone());
            self.manifest.add(entry)?;
            self.manifest.persist(&self.root)?;
        }
        Ok(result)
    }

    fn write_partition_file(
        &self,
        topic: &str,
        partition: i32,
        records: &[SourceRecord],
        ranges: &[OffsetRange],
        crash_point: CrashPoint,
    ) -> Result<FileEntry> {
        let first = records.first().context("partition batch is empty")?;
        let last = records.last().context("partition batch is empty")?;
        let received = DateTime::<Utc>::from_timestamp_nanos(first.received_at_ns_utc);
        let directory = self
            .root
            .join(format!("topic={}", safe_component(topic)))
            .join(format!("partition={partition}"))
            .join(format!("date={}", received.format("%Y-%m-%d")));
        fs::create_dir_all(&directory)
            .with_context(|| format!("create {}", directory.display()))?;
        let filename = format!(
            "offset-{:020}-{:020}.parquet",
            first.coordinate.offset, last.coordinate.offset
        );
        let final_path = directory.join(filename);
        let temporary_path = final_path.with_extension("parquet.tmp");
        if final_path.exists() {
            bail!("refusing to overwrite existing {}", final_path.display());
        }

        write_parquet(
            &temporary_path,
            records,
            &self.code_revision,
            &self.configuration_json,
            self.fixture_provenance.as_deref(),
        )?;
        File::open(&temporary_path)
            .with_context(|| format!("reopen {}", temporary_path.display()))?
            .sync_all()
            .with_context(|| format!("sync {}", temporary_path.display()))?;
        if crash_point == CrashPoint::BeforeParquetRename {
            bail!("injected crash before Parquet rename");
        }
        fs::rename(&temporary_path, &final_path).with_context(|| {
            format!(
                "atomically rename {} to {}",
                temporary_path.display(),
                final_path.display()
            )
        })?;
        sync_directory(&directory)?;
        if crash_point == CrashPoint::AfterParquetRenameBeforeManifest {
            bail!("injected crash after Parquet rename before manifest");
        }

        let sha256 = file_sha256(&final_path)?;
        Ok(FileEntry {
            path: relative_path(&self.root, &final_path)?,
            row_count: records.len(),
            sha256,
            created_at: Utc::now().to_rfc3339(),
            recorder_version: env!("CARGO_PKG_VERSION").to_owned(),
            code_revision: self.code_revision.clone(),
            configuration_json: self.configuration_json.clone(),
            fixture_provenance: self.fixture_provenance.clone(),
            ranges: ranges.to_vec(),
        })
    }
}

fn ensure_unique_and_ordered(records: &[SourceRecord]) -> Result<()> {
    for pair in records.windows(2) {
        if pair[0].coordinate.offset == pair[1].coordinate.offset {
            bail!(
                "duplicate source coordinate {}/{}/{} in one batch",
                pair[0].coordinate.topic,
                pair[0].coordinate.partition,
                pair[0].coordinate.offset
            );
        }
    }
    Ok(())
}

fn offset_ranges(records: &[SourceRecord]) -> Vec<OffsetRange> {
    let mut ranges: Vec<OffsetRange> = Vec::new();
    for record in records {
        match ranges.last_mut() {
            Some(range)
                if range.topic == record.coordinate.topic
                    && range.partition == record.coordinate.partition
                    && range.last_offset.checked_add(1) == Some(record.coordinate.offset) =>
            {
                range.last_offset = record.coordinate.offset;
            }
            _ => ranges.push(OffsetRange {
                topic: record.coordinate.topic.clone(),
                partition: record.coordinate.partition,
                first_offset: record.coordinate.offset,
                last_offset: record.coordinate.offset,
            }),
        }
    }
    ranges
}

fn parquet_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("schema_version", DataType::UInt16, false),
        Field::new("source_topic", DataType::Utf8, false),
        Field::new("source_partition", DataType::Int32, false),
        Field::new("source_offset", DataType::Int64, false),
        Field::new("kafka_timestamp_ms", DataType::Int64, true),
        Field::new("message_key", DataType::Binary, false),
        Field::new("exchange_code", DataType::Utf8, false),
        Field::new("security_id", DataType::Utf8, false),
        Field::new("received_at_ns_utc", DataType::Int64, false),
        Field::new(
            "exchange_event_time_epoch_seconds_raw",
            DataType::Int64,
            true,
        ),
        Field::new("packet_type", DataType::UInt8, false),
        Field::new("payload_json", DataType::Binary, false),
        Field::new("raw_packet", DataType::Binary, false),
    ]))
}

fn write_parquet(
    path: &Path,
    records: &[SourceRecord],
    code_revision: &str,
    configuration_json: &str,
    fixture_provenance: Option<&str>,
) -> Result<()> {
    let schema = parquet_schema();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt16Array::from(vec![
            PARQUET_SCHEMA_VERSION;
            records.len()
        ])),
        Arc::new(StringArray::from_iter_values(
            records
                .iter()
                .map(|record| record.coordinate.topic.as_str()),
        )),
        Arc::new(Int32Array::from_iter_values(
            records.iter().map(|record| record.coordinate.partition),
        )),
        Arc::new(Int64Array::from_iter_values(
            records.iter().map(|record| record.coordinate.offset),
        )),
        Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| record.kafka_timestamp_ms)
                .collect::<Vec<_>>(),
        )),
        Arc::new(BinaryArray::from_iter_values(
            records.iter().map(|record| record.message_key.as_slice()),
        )),
        Arc::new(StringArray::from_iter_values(
            records.iter().map(|record| record.exchange_code.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            records.iter().map(|record| record.security_id.as_str()),
        )),
        Arc::new(Int64Array::from_iter_values(
            records.iter().map(|record| record.received_at_ns_utc),
        )),
        Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| record.exchange_event_time_epoch_seconds_raw)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt8Array::from_iter_values(
            records.iter().map(|record| record.packet_type),
        )),
        Arc::new(BinaryArray::from_iter_values(
            records.iter().map(|record| record.payload_json.as_slice()),
        )),
        Arc::new(BinaryArray::from_iter_values(
            records.iter().map(|record| record.raw_packet.as_slice()),
        )),
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays).context("build Arrow record batch")?;
    let mut key_value_metadata = vec![
        KeyValue::new(
            "goldlab.parquet_schema_version".to_owned(),
            PARQUET_SCHEMA_VERSION.to_string(),
        ),
        KeyValue::new(
            "goldlab.recorder_version".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
        ),
        KeyValue::new("goldlab.code_revision".to_owned(), code_revision.to_owned()),
        KeyValue::new(
            "goldlab.configuration_json".to_owned(),
            configuration_json.to_owned(),
        ),
    ];
    if let Some(provenance) = fixture_provenance {
        key_value_metadata.push(KeyValue::new(
            "goldlab.fixture_provenance".to_owned(),
            provenance.to_owned(),
        ));
    }
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).context("configure Zstandard level")?,
        ))
        .set_key_value_metadata(Some(key_value_metadata))
        .build();
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer =
        ArrowWriter::try_new(file, schema, Some(properties)).context("create Parquet writer")?;
    writer.write(&batch).context("write Parquet row group")?;
    writer.close().context("close Parquet writer")?;
    Ok(())
}

fn recover_files(
    root: &Path,
    code_revision: &str,
    configuration_json: &str,
    fixture_provenance: Option<&str>,
    manifest: &mut Manifest,
) -> Result<RecoveryReport> {
    let mut report = RecoveryReport::default();
    let mut parquet_files = Vec::new();
    collect_output_files(root, &mut parquet_files, &mut report)?;
    parquet_files.sort();
    for path in parquet_files {
        let relative = relative_path(root, &path)?;
        if manifest.contains_path(&relative) {
            continue;
        }
        let (row_count, ranges) = inspect_parquet_coordinates(&path)?;
        let entry = FileEntry {
            path: relative,
            row_count,
            sha256: file_sha256(&path)?,
            created_at: Utc::now().to_rfc3339(),
            recorder_version: env!("CARGO_PKG_VERSION").to_owned(),
            code_revision: code_revision.to_owned(),
            configuration_json: configuration_json.to_owned(),
            fixture_provenance: fixture_provenance.map(ToOwned::to_owned),
            ranges,
        };
        manifest.add(entry)?;
        report.adopted_parquet_files += 1;
    }
    for entry in &manifest.files {
        let path = resolve_manifest_path(root, &entry.path)?;
        if !path.is_file() {
            bail!("manifest references missing file {}", path.display());
        }
        if file_sha256(&path)? != entry.sha256 {
            bail!("checksum mismatch for {}", path.display());
        }
    }
    Ok(report)
}

fn collect_output_files(
    directory: &Path,
    parquet_files: &mut Vec<PathBuf>,
    report: &mut RecoveryReport,
) -> Result<()> {
    for item in fs::read_dir(directory).with_context(|| format!("read {}", directory.display()))? {
        let path = item?.path();
        if path.is_dir() {
            collect_output_files(&path, parquet_files, report)?;
        } else if path.extension().is_some_and(|extension| extension == "tmp") {
            fs::remove_file(&path)
                .with_context(|| format!("remove incomplete {}", path.display()))?;
            report.removed_temporary_files += 1;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            parquet_files.push(path);
        }
    }
    Ok(())
}

fn inspect_parquet_coordinates(path: &Path) -> Result<(usize, Vec<OffsetRange>)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read Parquet metadata from {}", path.display()))?
        .build()
        .with_context(|| format!("build Parquet reader for {}", path.display()))?;
    let mut coordinates = Vec::new();
    for batch in reader {
        let batch = batch.with_context(|| format!("read rows from {}", path.display()))?;
        let topics = batch
            .column_by_name("source_topic")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .context("source_topic column missing or wrong type")?;
        let partitions = batch
            .column_by_name("source_partition")
            .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
            .context("source_partition column missing or wrong type")?;
        let offsets = batch
            .column_by_name("source_offset")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .context("source_offset column missing or wrong type")?;
        for row in 0..batch.num_rows() {
            coordinates.push(SourceRecord {
                coordinate: SourceCoordinate {
                    topic: topics.value(row).to_owned(),
                    partition: partitions.value(row),
                    offset: offsets.value(row),
                },
                kafka_timestamp_ms: None,
                message_key: Vec::new(),
                exchange_code: String::new(),
                security_id: String::new(),
                received_at_ns_utc: 0,
                exchange_event_time_epoch_seconds_raw: None,
                packet_type: 0,
                payload_json: Vec::new(),
                raw_packet: Vec::new(),
            });
        }
    }
    if coordinates.is_empty() {
        return Err(anyhow!("Parquet file {} contains no rows", path.display()));
    }
    coordinates.sort_by(|left, right| left.coordinate.cmp(&right.coordinate));
    ensure_unique_and_ordered(&coordinates)?;
    let row_count = coordinates.len();
    Ok((row_count, offset_ranges(&coordinates)))
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("open {} for checksum", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let bytes_read = file
            .read(&mut buffer)
            .with_context(|| format!("read {} for checksum", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        digest.update(&buffer[..bytes_read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn safe_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}
