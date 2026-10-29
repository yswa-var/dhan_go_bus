# Q001 — Parquet black box

## Scope

`goldlab-recorder` is one standalone Rust Kafka consumer group downstream of
the unchanged Go producer. It validates the Q000 value contract, preserves the
original Kafka value and decoded raw packet, writes Zstandard-compressed
Parquet, and commits source offsets only after the Parquet file and manifest
are durable.

This quest does not implement replay, market state, strategies, paper orders,
or live orders.

Build prerequisites are Rust stable and CMake (used to build vendored
librdkafka). DuckDB is optional and used only for inspection.

## Run

Start the existing Redpanda and topic, then run from the repository root:

```sh
make up
make topic
GOLDLAB_CODE_REVISION="$(git rev-parse HEAD)" \
  cargo run -p goldlab-recorder --release -- \
  --broker localhost:19092 \
  --topic dhan.market.raw \
  --group goldlab-recorder-v1 \
  --output recordings
```

Stop with Ctrl+C or SIGTERM. The recorder flushes the current non-empty batch,
syncs it, updates the manifest, commits its offsets, and exits. Relevant
environment variables have matching CLI flags:

| Setting | Default | Meaning |
| --- | --- | --- |
| `BROKER` | `localhost:19092` | Kafka-compatible broker |
| `TOPIC` | `dhan.market.raw` | Audited Q000 source topic |
| `RECORDER_GROUP` | `goldlab-recorder-v1` | Dedicated recorder consumer group |
| `RECORDER_OUTPUT` | `recordings` | Recorder-owned output root |
| `RECORDER_MAX_RECORDS` | `10000` | Hard maximum records buffered per batch |
| `RECORDER_MAX_BYTES` | `67108864` | Approximate uncompressed batch-byte rotation threshold |
| `RECORDER_ROTATE_SECONDS` | `60` | Time rotation threshold for a non-empty batch |
| `GOLDLAB_CODE_REVISION` | `unknown` | Revision stored in Parquet metadata and manifest |

The librdkafka receive queue is capped at 64 MiB. A single Kafka record can
cross the configured batch-byte threshold before the immediate rotation, but
the record-count limit remains fixed. Run only one recorder process against a
given output directory; concurrent manifest writers are not supported. If disk
finalization falls behind, polling pauses while the bounded client queue fills;
records remain at the broker rather than being silently dropped. Processing
longer than the configured five-minute max poll interval can cause a rebalance
and an observable commit failure.

## Output contract

Files are partitioned as:

```text
recordings/
  manifest.json
  topic=dhan.market.raw/
    partition=0/
      date=YYYY-MM-DD/
        offset-<first>-<last>.parquet
```

Every Parquet row has schema version 1:

| Column | Type | Semantics |
| --- | --- | --- |
| `schema_version` | `UINT16` | Recorder Parquet schema version |
| `source_topic` | UTF-8 | Kafka topic |
| `source_partition` | `INT32` | Kafka partition |
| `source_offset` | `INT64` | Kafka offset; deduplication identity with topic/partition |
| `kafka_timestamp_ms` | nullable `INT64` | Kafka CreateTime/LogAppendTime in Unix milliseconds |
| `message_key` | binary | Exact Kafka key bytes |
| `exchange_code` | UTF-8 | Normalized JSON identity field |
| `security_id` | UTF-8 | Normalized JSON identity field |
| `received_at_ns_utc` | `INT64` | Parsed JSON receive time in Unix nanoseconds UTC |
| `exchange_event_time_epoch_seconds_raw` | nullable `INT64` | Unadjusted Dhan LTT field; Q000 found its timezone/meaning unresolved |
| `packet_type` | `UINT8` | Dhan response code from raw byte 0 |
| `payload_json` | binary | Exact Kafka value bytes, preserving unknown future JSON fields |
| `raw_packet` | binary | Lossless bytes decoded from the JSON hex string |

Parquet footer metadata and each manifest file entry include recorder version,
code revision, serialized run configuration, schema version, and optional
fixture provenance. The manifest also stores file SHA-256, row count, and all
contiguous source-offset ranges. Identical raw market packets at different
Kafka offsets are retained; only an already-finalized topic/partition/offset
is deduplicated.

The recorder never deletes finalized data. Retention and archival are explicit
operator actions performed only after stopping the recorder; moving the whole
output directory preserves its manifest-relative paths.

Malformed JSON, a key/identity mismatch, invalid hex, raw length mismatch, or
raw/JSON security-ID mismatch stops the recorder with context. The offending
offset and later offsets are not committed.

## Durability and restart sequence

For each partition batch the recorder:

1. Writes and closes `*.parquet.tmp` with Zstandard compression.
2. Reopens and `fsync`s the temporary file.
3. Renames it to `*.parquet` within the same directory and `fsync`s that directory.
4. Writes, `fsync`s, and atomically renames `manifest.json.tmp`, then `fsync`s the output root.
5. Synchronously commits the next Kafka offset for each finalized partition.

Recovery behavior is deterministic:

- Crash before Parquet rename: startup removes the recorder-owned `.tmp`; Kafka
  replay rewrites the uncommitted coordinates.
- Crash after Parquet rename but before manifest: startup validates the Parquet
  footer, reconstructs exact coordinate ranges, checksums the file, and adopts
  it into a newly synced manifest.
- Crash after manifest but before Kafka commit: replayed coordinates are found
  in the manifest, not rewritten, and their offsets are committed.
- A manifest referencing a missing or checksum-mismatched Parquet file is a
  fatal observable error. It is never silently skipped.

Atomic rename is claimed only when the temporary and final paths remain on the
same filesystem, as they do under one output root. This is deterministic
at-least-once consumption with coordinate deduplication, not a claim of an
exactly-once transaction spanning Kafka and the filesystem.

## Inspect with DuckDB

With the `duckdb` CLI installed:

```sh
duckdb < scripts/inspect-recordings.sql
```

The queries report row/coordinate counts, source ranges, any duplicate source
coordinates, and raw-packet sizes. DuckDB reads the partitioned Parquet glob as
one relation; `hive_partitioning` is disabled because topic and partition are
already explicit columns.

## Test crash boundaries

```sh
cargo test -p goldlab-recorder
```

The deterministic Q000 fixture tests cover exact coordinates/raw bytes,
malformed input, bounded record rotation, crash before rename, crash after
rename before manifest, orphan adoption, and replay deduplication after durable
finalization. The Q000 input fixture contains exactly three rows and three
non-contiguous source coordinates. Kafka group commit ordering is implemented
in the binary and is not a cross-process transaction; a live broker run is an
operational check rather than a deterministic unit test.

## Known limitations

- There is no output-directory process lock; one writer per output root is an
  operational requirement.
- Atomic replacement depends on temporary and final files sharing a local
  filesystem with the tested rename and directory-sync semantics.
- Startup verifies every manifested file checksum, so recovery time grows with
  retained data size.
- Crash points are deterministically injected around filesystem finalization;
  automated tests do not kill a live Kafka consumer process mid-system-call.
- Dhan exchange-event timestamp semantics remain unresolved from Q000 and are
  stored only as the unadjusted raw epoch field.

