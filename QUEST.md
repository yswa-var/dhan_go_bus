# QUEST.md — GOLDLAB Factorio Research Tree

## North star
Build a reproducible MCX GOLD research factory: live Dhan market data enters through existing Go → Redpanda infrastructure; Rust records and replays events, maintains coherent futures/options state, computes rolling-window features, evaluates strategies, and simulates trades. Agents create strategies; humans approve paper and eventual live deployment.

**Existing foundation (done):** Dhan `.env` + TOTP token/cache, contract-master CSV, GOLD futures and ATM option selection, resolved-contract cache, rate-limited/retried LTP HTTP, `franz-go` Redpanda producer, JSON event including `raw_packet`, `exchange_code:security_id` message key, Redpanda + Console in Docker Compose, Makefile, `go test ./...` and `go vet ./...` passing.

**Important verification gap:** unit tests passing do not by themselves prove continuous WebSocket ingestion, all tick/depth fields, broker-ack durability or recovery. Verify actual behavior before assuming it exists.

## Gameplay rules
- Complete quests in order unless an explicit dependency allows parallel work.
- Every quest has one deliverable, explicit pass criteria and an evidence report.
- Nothing in future tiers is implemented early.
- Keep existing Dhan → Go → Redpanda code untouched by default.
- No Flink, Kubernetes, custom BroadcastBus, bespoke distributed scheduler, dynamic agent plugin system, or live trading during research quests.
- The stream and data contracts are more important than framework choice. Inspect the current repository rather than inventing fields or topics.

## Tech tree
```text
[BASE: Dhan → Go → Redpanda] ✓
           |
         Q000  Prove live feed contract
           |
         Q001  Parquet black box
           |
         Q002  Deterministic replay
           |
         Q003  Coherent GOLD market state
           |
         Q004  Rolling-window indicators
           |
         Q005  Rust strategy SDK
           |
         Q006  Paper broker + risk gate
           |
         Q007  Gold order-book buildup strategy
           |
         Q008  Codex strategy factory + evaluation
           |
         Q009  Shadow → guarded live promotion (separate approval)
```

---

## Q000 — Contract and live feed audit
**Unlock:** existing foundation. **Goal:** document the truth about the events already in Redpanda.

Build:
- Inspect actual topic names, schemas, message key, Dhan subscription coverage, `raw_packet` encoding, timestamps, offsets and Kafka producer acknowledgment settings.
- During an open market session, verify futures and subscribed CE/PE messages, including depth when subscribed and supported.
- Produce a small sanitized fixture set and a schema/semantics document.
- Record observable limitations (gaps on disconnect, missing depth, cumulative volume semantics, etc.).

Pass:
- One verified example per supported event type; exact command to inspect broker records.
- An explicit table of known/unknown fields and timestamps; no manufactured event data.
- Tests still pass; remaining unverified live behaviors are clearly labeled.

## Q001 — Parquet black box
**Depends:** Q000. **Goal:** preserve every successfully consumed event for later replay.

Build:
- One standalone Rust Kafka consumer group, separate from strategy consumers.
- Append-only compressed Parquet output with source topic/partition/offset, instrument identity, times, normalized payload and lossless `raw_packet`.
- Bounded batch buffer, file-size/time rotation, temporary file + atomic finalize when filesystem semantics permit.
- Durable file manifest/checkpoint describing finalized source-offset ranges; recover incomplete batches by replaying and deduplicating source coordinates.
- Graceful shutdown, errors, metrics and a DuckDB inspection query.

Pass:
- Exact fixture event count and source coordinates reproducible from Parquet.
- Forced crash before/after file finalization does not cause silent loss; duplicates are detected or resolved deterministically.
- Kafka offset commits occur only after output durability is established; no unbounded memory.

## Q002 — Deterministic historical replay
**Depends:** Q001. **Goal:** use recorded sessions without Dhan or live market hours.

Build:
- Parquet fixture reader yielding a canonical event type to the same domain logic used by live mode.
- Event ordering policy explicit per Kafka partition/instrument; record cross-instrument merge ordering and ambiguity.
- Replay speeds: `max` and optional wall-clock pacing; stable event IDs and deterministic output checksums.

Pass:
- Replaying the same immutable recording/config/code produces equivalent result rows and checksums.
- Missing/duplicate/out-of-order fixtures have documented outcomes.
- No lookahead information enters strategy views.

## Q003 — GOLD market state actor
**Depends:** Q002. **Goal:** one coherent current view of futures, CE and PE.

Build:
- Single mutable state owner for a GOLD instrument group, fed by canonical events.
- Maintain latest quote, five-level depth snapshots where available, rolling instrument metadata, received-at and event timestamps.
- Publish immutable `MarketView` snapshots with sequence/version, per-instrument freshness, and incomplete-data flags.
- Use bounded channels at I/O boundaries; avoid a goroutine/task per tick or strategy.

Pass:
- No data races; deterministic replay and snapshot order under documented ordering policy.
- Stale/missing instruments cannot silently appear current.
- State memory stays bounded under replay stress.

## Q004 — Sliding-window laboratory
**Depends:** Q003. **Goal:** reusable features, not a custom Flink clone.

Build:
- Explicit event-time rolling windows (e.g. 5s, 30s, 60s) with fixed slide/trigger policy.
- Features: resting bid/ask depth change, normalized depth imbalance, spread, mid, tick rate, rolling realized volatility; VWAP only if price/volume semantics support it.
- Warmup, zero denominators, late events, stale snapshots and gap policies documented.
- Golden-value unit tests using simple manual fixtures.

Pass:
- Known inputs produce exactly specified metrics, including window boundaries and out-of-order cases.
- Bounded per-instrument state and measured processing lag.

## Q005 — Small Rust strategy SDK
**Depends:** Q004. **Goal:** strategies are plain domain logic, not Kafka consumers.

Start with a minimal contract (shape may adapt to existing types):
```rust
pub trait Strategy: Send {
    fn id(&self) -> &str;
    fn on_market(&mut self, market: &MarketView) -> Vec<TradeIntent>;
}
```

Build:
- Versioned strategy identifier, typed config, declared required inputs and freshness limits.
- Pure decision boundary: no network calls, broker credentials, direct orders or filesystem effects.
- One reference toy strategy and fixture harness.

Pass:
- Two independent strategies run on identical replay input without modifying the market-state engine.
- Intent includes snapshot ID, instrument, side, valid lot quantity, reason and strategy version.

## Q006 — Paper broker and risk gate
**Depends:** Q005. **Goal:** realistically simulate trade intent execution.

Build:
- Explicit order lifecycle, portfolio positions, realized/unrealized P&L and immutable audit events.
- Fill model uses observed bid/ask and available depth conservatively; partial fills, latency, rejects, expiry/lot rules, fees, slippage.
- Deterministic risk controls: max lots, exposure, loss cap, data staleness rejection, cooldown, duplicate intent prevention, kill switch.

Pass:
- Deterministic order/fill/position tests and restart reconciliation.
- Paper mode cannot call Dhan live-order endpoints, including through configuration mistakes.

## Q007 — GOLD order-book buildup experiment
**Depends:** Q006. **Goal:** encode the first real strategy precisely.

Research hypothesis (not a validated edge): a persistent sudden buildup in displayed bid depth on an eligible option may correlate with favorable subsequent moves. The user example `delta = 0.3` is ambiguous until formally defined.

Specification required before coding:
- Define delta: e.g. `(current_bid_depth - baseline_bid_depth) / baseline_bid_depth >= 0.30`, **or** a different agreed quantity; do not confuse with option Greek delta.
- Define window, baseline, persistence, number of depth levels, and reset rules.
- Define ATM rule and CE/PE universe, volatility estimator/ranking, liquidity/spread filters and tie-breaking.
- Define paper entry, exit, stops, sizing, stale-feed response and max simultaneous positions.

Pass:
- Fixed fixtures distinguish persistent buildup from cancelled/spoofed resting orders where observable.
- Out-of-sample replay reports net P&L, drawdown, exposure, trade count, spread/slippage sensitivity and missing-data rate.
- Clearly state that a positive backtest does not prove deployability.

## Q008 — Codex strategy factory
**Depends:** Q007. **Goal:** describe → code → test → replay → review → paper.

Build:
- Strategy specification template and versioned folder conventions.
- One CLI/workflow to run format, clippy, tests, fixture replay, report generation and paper registration.
- Agent roles: builder, verifier, reviewer. Sequential roles are enough; no agent-swarm runtime required.
- Human approval before promoting any strategy to paper operation.

Pass:
- A second strategy is created without modifying ingestion, market state, paper broker or strategy SDK.
- Every experiment stores config, source revision, data range, results and known limitations.

## Q009 — Shadow and live execution (LOCKED)
**Depends:** Q008 plus separate explicit operator authorization. **Goal:** evaluate safe readiness for broker integration; not automatic deployment.

Before live authorization:
- Stable shadow operation and realistic paper results across multiple market conditions.
- Broker-specific order validation, exchange lot/tick restrictions, Dhan account/order state reconciliation, static-IP requirements as applicable.
- Risk gate enforced out-of-process or with independently tested protections, manual stop, daily loss ceiling and circuit breakers.
- Staged minimal-size roll-out with operator watching; no agent may enable live mode by itself.

Pass:
- Separate signed-off runbook and recovery drills. No live orders in automated tests.

---

## Standard quest acceptance report
```text
Quest ID:
Objective:
Files added/changed:
Data contract verified:
Commands run:
Observed results:
Failure/restart evidence:
Known limitations:
Ready for next quest? yes/no + justification
```

## Next action
Start **Q000** if live feed schema/subscriptions have not been demonstrated end-to-end. If Q000 evidence already exists, start **Q001**. Ask Codex to implement **only one quest** at a time.
