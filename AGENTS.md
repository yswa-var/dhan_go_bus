# AGENTS.md — GOLDLAB Engineering Constitution

## Mission
Build a small, reproducible, Rust-first research and paper-trading system for MCX GOLD futures and options **downstream of the existing Dhan → Go → Redpanda ingestion pipeline**.

The ingestion layer already exists. Do not replace or refactor it without an explicitly assigned quest.

## Design philosophy
Follow the Zen of Python as engineering guidance, regardless of language:

> Beautiful is better than ugly. Explicit is better than implicit. Simple is better than complex. Complex is better than complicated. Flat is better than nested. Sparse is better than dense. Readability counts. Special cases aren't special enough to break the rules. Although practicality beats purity. Errors should never pass silently. Unless explicitly silenced. In the face of ambiguity, refuse the temptation to guess. There should be one-- and preferably only one --obvious way to do it. Although that way may not be obvious at first unless you're Dutch. Now is better than never. Although never is often better than *right* now. If the implementation is hard to explain, it's a bad idea. If the implementation is easy to explain, it may be a good idea. Namespaces are one honking great idea -- let's do more of those!

Practical consequences:
- Prefer existing maintained libraries to custom frameworks.
- One canonical schema, one consumer implementation, one clear source of truth for each state.
- Small modules, explicit domain types, short error paths, and descriptive names.
- No speculative microservices, orchestration, plugin architecture, or custom message bus.
- Introduce an abstraction only after a second concrete use case demands it.
- Never claim reliability, determinism, or exactly-once behavior without tests proving the scope of that claim.

## Immutable boundary
- Existing: Dhan authentication, contract resolution, WebSocket/HTTP ingestion, Go normalizer, `franz-go` producer, Redpanda, Console, Docker Compose, Makefile.
- Inspect the repository to discover actual file paths, topic names, event fields, timestamp semantics, and message contracts. **Never invent them.**
- Do not modify the upstream pipeline unless the active quest explicitly requires it; prefer a narrow compatibility adapter downstream.

## Preferred stack
- Rust stable, Tokio, an established Kafka-compatible Rust client, Arrow/Parquet, DuckDB for ad hoc analysis.
- Redpanda remains the event log and replay source. No Flink, Kubernetes, custom BroadcastBus, or hand-built distributed engine.
- Keep dependencies minimal and justify new ones.

## Runtime boundaries
1. **Recorder** consumes Redpanda and writes auditable Parquet recordings.
2. **Replay** reads recorded events and feeds the same market-state code as live mode.
3. **Market state** has one mutable owner per GOLD instrument group; strategy inputs are coherent, immutable, versioned snapshots.
4. **Strategies** express decisions as `TradeIntent`, never call a broker directly.
5. **Execution** validates intents through a central risk gate. Paper mode is the only permitted execution mode until a separate live-trading authorization quest.
6. **Agents** write/review/test code offline. Never put an LLM in the low-latency tick-to-order path.

## Market-data correctness
- Carry exchange/security identity, exchange event time (if present), ingest time, and Kafka topic/partition/offset separately.
- Explicitly define ordering, duplicate handling, missing timestamps, late events, staleness, and reconnect gaps.
- `raw_packet` must be preserved losslessly if provided upstream; document its encoding.
- A five-level snapshot is not a 200-level book, and resting quantity is not executed volume.
- Use a clear definition for *delta*: depth change, order-flow delta, or option Greek delta; never interchange them.
- For cross-instrument strategies, show per-instrument freshness and prohibit decisions on stale or incomplete snapshots.

## Data durability
- Never commit Kafka offsets before the corresponding output is durably finalized.
- Exactly-once Parquet output is **not** automatically guaranteed by Kafka consumer groups. Use explicit source-offset manifests and deterministic deduplication/recovery rules; test crash boundaries.
- Use bounded buffers, file-size/time rotation, atomic finalization where supported, partitioned data, and retention policy.
- Store versioned schemas, config, strategy revision, fixture provenance, and code revision for reproducibility.

## Concurrency rules
- Async for network and disk I/O; bounded channels only at clear ownership boundaries.
- No Tokio task per tick, per rolling window, or per strategy by default.
- Keep instrument-group state updates serialized; parallelize independent research or snapshot evaluations only if benchmarks justify it.
- Document behavior when consumers fall behind; no silent drops or unbounded allocations.

## Risk and security
- Paper trading by default; never send real orders as part of tests or agent-generated code.
- Live execution requires separate operator approval, guarded configuration, kill switch, limit controls, order-state reconciliation, and an explicit promotion quest.
- No secrets in source, logs, fixtures, or reports. Use environment variables or an existing secret mechanism.
- No profit claims based solely on backtests. Use spread, slippage, fees, partial fills, and out-of-sample evaluation.

## Definition of done for any quest
- Read `QUEST.md`, `CODEX.md`, and this file first.
- Restate the quest's scope and inspect the existing code/schema before editing.
- Deliver the smallest vertical slice that satisfies stated acceptance criteria.
- Include deterministic tests, a concise README or run instructions, and an observable failure path.
- Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` for Rust changes. Run `go test ./...` and `go vet ./...` for any Go changes.
- State what was actually executed, what was not verified, and why.
- Stop after the requested quest. Do not auto-start the next one.
