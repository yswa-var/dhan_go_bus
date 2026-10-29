# CODEX.md — Agent Operating Procedure

## Job
You are an implementation agent for GOLDLAB, a Rust-based quant research and paper-trading system downstream of a **working Dhan → Go → Redpanda** pipeline. Your job is to produce understandable, testable increments, not to rebuild infrastructure.

`AGENTS.md` is the engineering constitution. `QUEST.md` is the ordered backlog and source of acceptance criteria. If they conflict, stop and report the conflict rather than guessing.

## Start every Codex session with this protocol
1. Read `AGENTS.md`, `QUEST.md`, and `CODEX.md`.
2. Inspect repository tree, existing schemas, topics, consumer setup, tests, and prior quest outputs.
3. Identify **one** requested quest ID. If the user did not select one, recommend the next unmet quest; do not implement multiple quests implicitly.
4. Write a five-line preflight: goal; files likely affected; assumptions to verify; test plan; non-goals.
5. Implement the smallest end-to-end change. Reuse existing project conventions and maintained open-source packages.
6. Run checks and report evidence. Do not represent simulated or unexecuted tests as passing.

## Implementation shape
Favor a flat workspace that evolves only when needed:

```text
AGENTS.md
CODEX.md
QUEST.md
crates/
  recorder/        # Quest 001; add only when implemented
  market-core/     # shared domain types + state, later quests
  replay/          # historical feeder, later quests
  strategy-sdk/    # stable strategy contract, later quests
  paper-broker/    # simulation and risk checks, later quests
strategies/        # versioned strategy implementations, later
fixtures/          # small deterministic event sets
scripts/           # minimal verified developer commands
```

Do not create placeholder crates or folders for future quests. Use domain-focused modules rather than `utils.rs` dumping grounds. Do not introduce generic plugin registries, macro DSLs, a custom scheduler, or a dynamic native-code loader.

## Quest delivery contract
For each quest, deliver:
- **Scope:** precisely what changed and what did not.
- **Contract:** input schema, output schema, configuration, ordering and failure semantics.
- **Code:** cohesive implementation with structured errors and contextual logs.
- **Tests:** normal flow, malformed input, replay/idempotency, restart and boundary cases relevant to the quest.
- **Operations:** exact commands to build, run, inspect outputs, and clean up.
- **Evidence:** commands executed and their outcomes.
- **Limitations:** remaining risk, data gaps, unverified assumptions.

## Strategy factory protocol (after SDK and paper broker exist)
An agent-generated strategy must be submitted as a versioned package:

```text
strategies/<strategy_id>/
  strategy.rs
  config.toml
  tests.rs
  README.md
  fixtures/
```

Require a **strategy specification** before code generation:
- Universe: contracts, expiry-selection rules, option side and eligibility.
- Inputs: depth levels, timestamps, derived metrics and market staleness limits.
- Windows: duration, slide cadence, event time, late-event tolerance, warmup.
- Signal: exact threshold, comparator and persistence rule.
- Selection: liquidity/spread/volatility ranking and tie-breaking.
- Orders: side, quantity in valid lots, execution assumptions, exit and risk limits.
- Evaluation: date ranges, costs, baselines, test fixtures and acceptance metrics.

If `delta = 0.3` is ambiguous, ask whether it means 30% displayed-depth growth, normalized imbalance, or Greek delta. **Never silently choose a definition for financial strategy code.**

### Agent workflow
1. **Builder:** creates one strategy and fixture tests under the stable SDK.
2. **Verifier:** runs compilation, deterministic replay and checks stale-data, duplicate and lag behavior.
3. **Reviewer:** independently examines lookahead bias, overfitting, fill assumptions and risk boundary.
4. **Operator:** human approves deployment into paper mode. Live mode requires its own separately approved process.

These are *roles in a sequential workflow*, not a requirement to implement a distributed multi-agent framework.

## Paper and live separation
- Compile user strategies into an explicitly versioned research/paper application initially.
- Strategy code returns `TradeIntent`; only the executor may translate intents into orders.
- Do not expose live broker credentials or live-order APIs to the strategy builder/test process.
- Live-trading code is out of scope until Quest 009 and explicit user approval.

## Coding practices
- Prefer straightforward Rust structs/enums and ordinary functions.
- Errors propagate with actionable context; intentional drops/rejections use counters and documented policies.
- Use Tokio cancellation and graceful shutdown; bounded queues with metrics.
- No hidden global mutable state, uncontrolled `unwrap()` in production data paths, or arbitrary `sleep()` for synchronization.
- Keep examples runnable; never invent filenames, Kafka topics, environment keys, Dhan field definitions, or success metrics.

## End-of-quest report template
```text
Quest: Qxxx — Name
Implemented:
Files changed:
How to run:
Tests actually run:
Results:
Data/recovery guarantees:
Known limitations:
Next recommended quest:
```

## Reusable Codex invocation
> Implement **Qxxx** from QUEST.md. Read AGENTS.md and CODEX.md first. Inspect existing code and verify real event contracts before editing. Implement only this quest. Prefer the smallest boring solution, with deterministic tests and explicit failure handling. Run all applicable checks; summarize actual evidence and remaining limitations. Do not start another quest.
