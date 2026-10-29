# Q003 coherent GOLD market state

Q003 adds one deterministic state owner for a configured GOLD-family
instrument group. The owner accepts the canonical `MarketEvent` used by live
recording and historical replay and publishes one coherent, immutable
`MarketView` after each accepted event. It does not compute rolling indicators,
run strategies, or place orders.

## Verified input boundary

The decoder accepts only the Q000-verified MCX full-feed contract:

- Dhan packet code 8.
- Exactly 162 lossless raw bytes.
- MCX segment byte 5.
- Quote fields and exactly five regular depth levels using the byte layout in
  the Q000 audit.

Ticker-only and quote-only packets are rejected even though the upstream Go
decoder contains paths for them: Q000 captured no live examples. This is a
five-level resting-depth snapshot, not a 20/200-level book and not executed
volume. The raw volume, last-trade quantity, average-price, and OI semantics
remain only as verified in Q000.

Instrument roles are loaded from the Go-generated `resolved-contracts-*.csv`.
Security IDs are never guessed. Only `MCX_COMM` GOLD-family `FUTCOM` futures and
`OPTFUT` CE/PE rows are accepted; duplicate IDs and unsupported rows fail.

## State and snapshot contract

One `GoldMarketState` owns all mutable state for its configured group. The
Tokio actor wrapper runs exactly one task and connects through bounded event
and snapshot channels. A full snapshot is sent with backpressure after each
event; there is no task per tick or instrument and no silent drop path.

Every version-1 `MarketView` contains:

- A monotonically increasing sequence and triggering stable event ID.
- A monotonic `as_of_received_at_ns_utc` watermark.
- Every configured instrument in security-ID order.
- Static contract metadata and latest source topic/partition/offset.
- Ingest receive time and the separate raw exchange timestamp.
- Latest quote and exactly five depth levels when present.
- Per-instrument `fresh`, `stale`, or `missing` status.
- Explicit incomplete reasons and a whole-view `complete` flag.
- Cumulative observed source-offset gap boundaries.

Freshness uses only ingest `received_at`, whose semantics were verified in
Q000. The watermark is the maximum receive time seen so far, so cross-partition
time regression cannot make the view clock move backward. An instrument is
stale when `age_ns > stale_after_ns`; equality remains fresh. The unresolved
Dhan timestamp is preserved as
`exchange_event_time_epoch_seconds_raw` but never drives freshness or ordering.

The view is complete only when every configured instrument has a quote, a
five-level snapshot, and fresh data. Missing or stale futures, CE, or PE
therefore cannot silently appear current.

## Ordering and failure behavior

- Configured source partitions are fixed, bounding ordering state.
- Each source partition must have strictly increasing Kafka offsets.
- Duplicate or decreasing offsets fail before state advances.
- Offset jumps are accepted and counted as gap boundaries; missing market data
  cannot be reconstructed.
- Unknown instruments, unconfigured sources, malformed packets, non-finite
  prices, and unverified packet types fail before publication.
- Cross-partition merge order remains the explicit Q002 replay order. It is
  deterministic, not claimed to be exchange chronology.

## Human visual proof from a recording

Use the resolved-contract cache that belongs to the recording session. Choose
the staleness threshold explicitly; the example uses 30 seconds and does not
claim that this is a future strategy's correct threshold.

```sh
cargo run -p goldlab-replay --bin goldlab-market-state --release -- \
  --input "${GOLDLAB_TEST_OUTPUT}" \
  --contracts resolved-contracts-20261029T100331Z.csv \
  --group-id GOLD-manual-test \
  --stale-after-ms 30000 \
  --source-topic dhan.market.raw \
  --source-partitions 0 \
  --last-view-output /tmp/goldlab-last-view.json \
  | tee /tmp/goldlab-state-summary.json
```

The summary reports configured instrument/source counts, event and final
sequence counts, final fresh/stale/missing counts, offset gaps, receive-time
regressions, a configuration hash, a snapshot-stream hash, and a final-view
hash.

Inspect the final view:

```sh
jq '{
  sequence,
  complete,
  as_of_received_at_ns_utc,
  source_offset_gaps,
  instruments: (.instruments | length),
  fresh: ([.instruments[] | select(.freshness.status == "fresh")] | length),
  stale: ([.instruments[] | select(.freshness.status == "stale")] | length),
  missing: ([.instruments[] | select(.freshness.status == "missing")] | length),
  kinds: ([.instruments[].metadata.kind] | group_by(.) |
          map({kind: .[0], count: length}))
}' /tmp/goldlab-last-view.json
```

Visual pass conditions:

- `event_count` equals `final_sequence`.
- Instrument count equals the selected resolved-contract rows.
- `complete` is false whenever stale or missing count is nonzero.
- The kind counts agree with the contract cache, rather than an inferred ID
  pattern.
- Running the same recording, contracts, group, staleness threshold, source
  list, and code twice produces identical configuration, snapshot-stream, and
  final-view hashes.

Automated verification:

```sh
cargo test -p goldlab-market-core --locked
cargo test -p goldlab-replay --locked
```

Tests use the exact Q000 future/CE/PE packets. They cover golden quote/depth
values, coherent completion, stale and missing flags, receive-time regression,
duplicate/decreasing offsets, unknown instruments, unsupported packets,
bounded actor channels, deterministic replay snapshots, and 10,000 updates
without state-map growth.

## Observed Q003 audit evidence

The preserved 2026-10-29 Q001 recording was replayed twice with its 48-row
resolved-contract cache and a declared 30-second threshold:

| Check | Observed result |
| --- | ---: |
| Events / final sequence | 71,188 / 71,188 |
| Configured instruments | 48 |
| Futures / calls / puts | 4 / 22 / 22 |
| Final fresh / stale / missing | 48 / 0 / 0 |
| Source gap boundaries | 0 |
| Receive-time regressions | 0 |
| Configuration SHA-256 | `9ab5f858f7f229487a491897b853669d0059340f82d127880980f5cf4b538b8d` |
| Snapshot-stream SHA-256 | `fa9e32fac5b03d2b15d4a71cc7d4046932c0497488344a9b20399b3ff1a0b487` |
| Final-view SHA-256 | `359eee248e3b8f407f450baa533752ea5c82ccde866fd7466dfb52278a180e4a` |

These observations describe that recording under the chosen threshold. They
do not establish continuous-feed behavior or a universally correct staleness
limit.

## Limitations

- Only packet code 8 from the verified MCX session is supported.
- The actor blocks on a full snapshot channel. This preserves data but means a
  slow consumer slows ingestion; capacity must be chosen operationally.
- Each snapshot copies the fixed configured instrument set. Memory is bounded
  by instrument count and channel capacities, but snapshot cost is O(N).
- Contract changes require an explicit new state configuration; Q003 does not
  roll expiries or change the option universe automatically.
- No reconnect-gap marker exists upstream. Only Kafka offset jumps are visible.
- No Q004 indicators or trading decisions are present.
