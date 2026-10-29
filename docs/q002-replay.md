# Q002 deterministic historical replay

Q002 reads a finalized Q001 recording without Dhan or Redpanda. It validates
the manifest and every Parquet checksum, reconstructs the same canonical
`MarketEvent` used by the live recorder, and emits deterministic JSON Lines.
It does not maintain a market view, evaluate a strategy, or place orders.

## Run it

Fast replay writes one result row per canonical event:

```sh
cargo run -p goldlab-replay --release -- \
  --input recordings/manual-YYYYMMDDTHHMMSSZ \
  --speed max \
  --output replay.jsonl
```

The command prints a JSON summary to stdout. To validate without retaining
rows, omit `--output`; rows are still decoded and checksummed but sent to an
I/O sink. Memory is bounded by `--batch-records` (default 1024), plus one
serialized result row.

Wall-clock pacing is optional:

```sh
cargo run -p goldlab-replay --release -- \
  --input recordings/manual-YYYYMMDDTHHMMSSZ \
  --speed wall-clock \
  --output replay-wall-clock.jsonl
```

Wall-clock mode delays by the positive difference between adjacent
`received_at_ns_utc` values. Equal or regressing values cause no delay and
increment `received_time_regressions`. Pacing never uses the unresolved Dhan
last-trade-time field. Speed changes timing only; it does not change rows or
the event checksum.

## Ordering and stable identity

The versioned policy is `topic_partition_offset_v1`:

1. Topic names are ordered lexicographically.
2. Partitions within a topic are ordered numerically.
3. Events within each partition must have strictly increasing Kafka offsets.

This preserves Kafka's only native ordering guarantee. It deliberately makes
no claim that one partition's event happened before another partition's event.
Consequently, a cross-instrument merge is deterministic but not chronological
when instruments occupy different partitions. An instrument's order is the
subsequence of its records in this deterministic partition traversal.

The stable event ID is `<topic>/<partition>/<offset>`. The replay checksum is
SHA-256 over a version marker, the ordering-policy ID, and length-delimited
canonical fields for every event in replay order. It includes source identity,
timestamps, message key, normalized identity, original JSON bytes, and lossless
raw-packet bytes. The summary also records the SHA-256 of `manifest.json`.

## Missing, duplicate, and out-of-order input

| Condition | Outcome |
| --- | --- |
| Missing offset inside a partition | Replay continues; `offset_gaps` and `missing_offsets` report it. |
| Duplicate offset | Replay fails at that row; no later row is emitted. |
| Decreasing offset | Replay fails at that row; no later row is emitted. |
| Overlapping/unordered manifest ranges | Reader construction fails before replay. |
| Missing or unmanifested Parquet file | Reader construction fails before replay. |
| Parquet checksum mismatch | Reader construction fails before replay. |
| Leftover `.tmp` file | Reader construction fails before replay. |
| Row disagrees with its payload/raw packet | Replay fails when that row is decoded. |

Gaps are observable but cannot be reconstructed. A successful replay is not a
claim that the source session captured every exchange event.

## No-lookahead boundary

Domain consumers receive exactly one immutable canonical `MarketEvent` at a
time. The callback exposes no following row, batch, file metadata, final price,
or session aggregate. Arrow may decode a bounded batch internally for I/O
efficiency, but future rows are never passed into the current domain call.
Q003 will build market state on this single-event boundary; Q002 does not add
that future state early.

## Human verification

Run the same immutable recording twice:

```sh
cargo run -p goldlab-replay --release -- \
  --input "${GOLDLAB_TEST_OUTPUT}" --speed max --output /tmp/replay-a.jsonl \
  > /tmp/replay-a-summary.json
cargo run -p goldlab-replay --release -- \
  --input "${GOLDLAB_TEST_OUTPUT}" --speed max --output /tmp/replay-b.jsonl \
  > /tmp/replay-b-summary.json
shasum -a 256 /tmp/replay-a.jsonl /tmp/replay-b.jsonl
diff -u /tmp/replay-a-summary.json /tmp/replay-b-summary.json
```

Visual pass condition: both JSONL SHA-256 values are identical, `diff` prints
nothing, and both summaries contain the same `checksum_sha256`, event count,
gap counts, manifest hash, and ordering policy.

Inspect the first rows without exposing secrets:

```sh
head -n 3 /tmp/replay-a.jsonl | jq \
  '{sequence,event_id,exchange_code,security_id,packet_type}'
```

Run the deterministic acceptance tests:

```sh
cargo test -p goldlab-replay --locked
```

These cover byte-identical repeat replay, the exact three Q000 source
coordinates, gap accounting, cross-partition ambiguity, checksum failure,
unmanifested files, duplicate/decreasing offsets, and pacing regressions.

## Limitations

- The reader accepts only Q001 manifest and Parquet schema version 1.
- Determinism assumes immutable recording bytes, configuration, ordering policy,
  replay code, and dependency behavior.
- Wall-clock mode can take the original session duration and is not a real-time
  scheduler; operating-system timing jitter is expected.
- The whole manifest and file plan are held in memory, while event data stays
  bounded by the Arrow batch size.
- Stable IDs use `/` separators without escaping. Current topic names cannot
  contain `/` under Kafka naming rules.
