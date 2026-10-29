# Dhan → Go → Redpanda (V1)

Minimal, local-first ingestion only. No custom bus, Flink, worker jobs, database, or trading logic.

## Run the live feed

Provide a root `.env` with `CLIENT_ID`, `TOTP_KEY`, and `DPIN`. Dhan market data requires an active data plan. Start Redpanda and create the topic:

```sh
make up
make topic
go run ./dhan-redpanda-starter/cmd/ingest
```

The command downloads `contracts.csv` if absent, resolves the nearest GOLDM, GOLD, GOLDPETAL, and GOLDGUINEA futures, and uses live GOLDM and GOLD futures quotes to select five strikes on each side of ATM (CE and PE). It subscribes to all 48 contracts in Dhan's full feed mode and publishes one keyed message per received market packet to `dhan.market.raw`. Records contain an exchange code, security ID, UTC receive time, and hex encoded raw packet. Set `BROKER` and `TOPIC` or pass `-broker` and `-topic` to override the defaults. Use `-contracts` to point to an existing contract master CSV.

Stop with Ctrl+C. Publishing waits for Kafka acknowledgement; a failed publish stops ingestion. The feed reconnects and resubscribes after connection errors. The current selection is refreshed on process restart after its 12 hour cache expires; it does not shift ATM strikes while running.
