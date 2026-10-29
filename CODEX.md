# Codex contract

Dhan WebSocket v2

Live ticks • Quotes • Market depth

Go Ingestion Service

Connect • Decode • Normalize • Publish

Redpanda

Durable event log • Retention • Replay

## V1 scope

1. Connect to Dhan WebSocket.
2. Decode and normalize market packets in Go.
3. Publish to Redpanda using Kafka protocol.
4. Partition by `exchange:security_id` to preserve per-instrument order.
5. Configure retention, producer acknowledgments, and retries.
6. Verify messages using Redpanda Console.

Not building yet: Flink, workers, sliding windows, indicators, database, or custom message buses.

Principle: Go produces reliable market events. Redpanda stores them for future consumers.

One caveat: Redpanda can retain only successfully ingested events. Dhan connection failures may still create gaps.

This is our V1 architecture.
