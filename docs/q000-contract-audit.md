# Q000 — Contract and live feed audit

Audit date: 2026-10-29. Repository revision inspected: `3f362ab`. The three
fixture records are exact `rpk` envelopes copied from the local broker; no
market field was synthesized or changed.

## Scope and evidence

The retained session covers Kafka partition 0 offsets 0 through 71187
(71,188 records), from `2026-10-29T10:03:36.612178Z` through
`2026-10-29T10:07:59.326034Z`. The resolved-contract cache generated at
`2026-10-29T10:03:31Z` contains 48 MCX subscriptions: four futures, 22 CE and
22 PE contracts. Every one of those 48 security IDs appears in the retained
session. Record counts ranged from 68 to 2,367 per instrument.

All 71,188 retained records contain packet code 8 (Dhan full mode) and a
162-byte raw packet. This is one supported event type in this captured session:
a full quote with five levels of regular market-feed depth. It is not Dhan's
separate 20/200-level FullDepth feed, which Dhan documents only for NSE equity
and derivatives, not MCX. The fixtures contain one GOLD future, one GOLDM CE,
and one GOLDM PE example:

| Kind | Security ID | Topic/partition/offset |
| --- | --- | --- |
| GOLD future | `495213` | `dhan.market.raw/0/71181` |
| GOLDM CE | `581902` | `dhan.market.raw/0/71088` |
| GOLDM PE | `582003` | `dhan.market.raw/0/71156` |

Ticker (code 2) and quote (code 4) decoding exists in Go, but the running
ingestor requests code 21 (full mode) for every subscription. No ticker-only or
quote-only broker record was observed, so no live fixture is claimed for them.
Dhan also documents previous-close, OI, and market-status response codes; the
current decoder ignores unhandled codes rather than publishing them.

The binary layout names below were cross-checked against Dhan's official
[Live Market Feed v2 specification](https://dhanhq.co/docs/v2/live-market-feed/)
and [annexure](https://dhanhq.co/docs/v2/annexure/). Observed values and
coverage claims come from the local broker, not from documentation examples.

## Kafka contract

Defaults may be overridden by `BROKER`, `TOPIC`, `-broker`, and `-topic`.

| Property | Verified value | Source |
| --- | --- | --- |
| Default topic | `dhan.market.raw` | CLI default and live broker |
| Partitions / replicas | 1 / 1 | `rpk topic describe` |
| Message key | ASCII `MCX:<security_id>` | publisher source and broker records |
| Partition ordering | Kafka offset order within partition 0 only | broker coordinates |
| Kafka timestamp | client CreateTime in Unix milliseconds | topic config and records |
| Value encoding | UTF-8 JSON object | publisher source and records |
| Producer call | synchronous `ProduceSync` per event | publisher source |
| Required acknowledgements | franz-go v1.22.2 default `AllISRAcks` (`-1`) | installed dependency source; no override |
| Idempotent producer | franz-go default enabled | installed dependency documentation; no override |
| Broker retention | delete policy, 604800000 ms (7 days) | live topic config |

`ProduceSync` success proves the configured acknowledgement completed. With
one replica, `AllISRAcks` still acknowledges only that single in-sync replica;
it does not provide replica-failure tolerance or end-to-end exactly-once
delivery. Producer idempotency addresses retries within a producer session,
not duplicates already emitted by the upstream feed or after process restart.

### JSON value schema

| Field | Type | Status and semantics |
| --- | --- | --- |
| `exchange_code` | string | Known: publisher writes constant `MCX`. It is not copied from the decoded segment. |
| `security_id` | string | Known: decimal Dhan security ID; matches raw bytes 4..7 (little-endian u32). |
| `received_at` | string | Known: UTC RFC3339Nano from `time.Now()` immediately before synchronous publish; ingest time, not exchange time. |
| `raw_packet` | string | Known: lowercase, unprefixed hex; two characters per original byte. Live full packets decode to exactly 162 bytes. |
| event type | absent | Known limitation: recover from raw byte 0; full is code 8. |
| decoded quote/depth | absent | Known limitation: decoder creates typed views in memory but publisher stores only the raw packet. |
| topic/partition/offset | broker metadata | Known: available from Kafka, not duplicated inside the JSON value. |

No schema-registry subject or explicit payload schema version is configured.

## Full-packet fields decoded by this repository

All multi-byte values are little-endian. Float fields are IEEE-754 float32.
Half-open byte ranges refer to the decoded 162-byte `raw_packet`.

| Bytes | Field | Type | Status / semantics |
| --- | --- | --- | --- |
| 0..1 | packet code | u8 | Known: 8 in every retained record (full). |
| 1..3 | packet length | u16 | Known: 162 in every retained record. |
| 3..4 | exchange segment | u8 | Known: 5 (`MCX_COMM`) in every retained record. |
| 4..8 | security ID | u32 | Known and checked against JSON/key. |
| 8..12 | LTP | f32 | Decoded as last traded price. |
| 12..14 | last trade quantity | u16 | Decoded; per-trade/cumulative behavior not independently verified. |
| 14..18 | timestamp | u32 | Decoded as Unix seconds by current Go code, but observed values are later than ingest time; timezone/meaning is unresolved and must not yet drive event-time logic. |
| 18..22 | average price | f32 | Decoded; averaging semantics not independently verified. |
| 22..26 | volume | u32 | Decoded; likely session cumulative, but not verified from broker documentation or a controlled trade. |
| 26..30 | total sell quantity | u32 | Decoded aggregate resting quantity; not executed volume. |
| 30..34 | total buy quantity | u32 | Decoded aggregate resting quantity; not executed volume. |
| 34..38 | open interest | u32 | Decoded. |
| 38..42 | day high OI | u32 | Officially defined only for NSE_FNO; ignored by this decoder and not semantically supported for these MCX records. |
| 42..46 | day low OI | u32 | Officially defined only for NSE_FNO; ignored by this decoder and not semantically supported for these MCX records. |
| 46..50 | open | f32 | Decoded. |
| 50..54 | close | f32 | Decoded. |
| 54..58 | high | f32 | Decoded. |
| 58..62 | low | f32 | Decoded. |
| 62..162 | five depth levels | 5 x 20 bytes | Each level: bid qty u32, ask qty u32, bid orders u16, ask orders u16, bid price f32, ask price f32. Snapshot/resting depth, not trades. |

The typed `FeedEvent.ReceivedAt` currently receives the packet timestamp during
decoding, despite its name. That value is discarded by the publisher, which
correctly creates the JSON `received_at` from the local UTC clock.

## Ordering, duplicates, gaps, and failure semantics

- All observed records are in one partition, so offsets define a total order
  for this session. The key would preserve per-instrument order if the topic is
  later expanded, but no cross-partition order should be inferred.
- Only 12,258 distinct raw packet byte strings occur among 71,188 records.
  Repeated byte-identical packets are therefore observable and must not be
  assumed unique. Their origin (feed behavior, reconnect replay, or another
  cause) was not established by this audit.
- The ingestor stops if synchronous publishing returns an error. It reconnects
  and resubscribes after feed read/connect failures with exponential backoff.
  It stores no Dhan sequence number, gap marker, or resume cursor, so events
  missed during disconnects cannot be detected or recovered from this topic.
- An unhandled packet code is silently ignored by the decoder, including Dhan's
  documented previous-close (6), market-status (7), and standalone OI (5)
  packets. A malformed known packet ends the read, then triggers reconnect.
  These paths have unit coverage but were not induced against the live service.
- Contract selection is fixed for the process lifetime. ATM options do not
  shift until restart; the cache is accepted for 12 hours.

## Exact inspection commands

Run from the repository root while Compose is available:

```sh
docker compose exec -T redpanda rpk topic describe dhan.market.raw --brokers redpanda:9092 --print-partitions
docker compose exec -T redpanda rpk topic consume dhan.market.raw --brokers redpanda:9092 --offset 71088:71089 --format json --pretty-print=false
docker compose exec -T redpanda rpk topic consume dhan.market.raw --brokers redpanda:9092 --offset 71156:71157 --format json --pretty-print=false
docker compose exec -T redpanda rpk topic consume dhan.market.raw --brokers redpanda:9092 --offset 71181:71182 --format json --pretty-print=false
```

To inspect future records without relying on these historical offsets:

```sh
docker compose exec -T redpanda rpk topic consume dhan.market.raw --brokers redpanda:9092 --offset -10 --num 10 --format json --pretty-print=false
```

## Explicitly unverified

- Continuous operation beyond the 4 minute 22.714 second retained interval.
- Disconnect duration, data loss across reconnect, and resubscription success
  in a live failure.
- Dhan acknowledgement of each requested subscription; coverage is inferred
  from at least one record for every selected security ID.
- Correct semantic/timezone interpretation of raw bytes 14..17.
- Whether volume, last-trade quantity, average price, and OI fields exactly
  match Dhan's intended MCX semantics.
- Broker durability under crash or replica failure, and any exactly-once claim.
- Ticker-only, quote-only, 20-level, or 200-level depth events.
