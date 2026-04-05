# Flink Clickstream Event Reordering

A Apache Flink 2.2 streaming application that reads clickstream events from Kafka, tolerates up to 5 minutes of out-of-order delivery, emits a `CheckoutSession` record whenever a qualifying checkout sequence is detected, and aggregates checkout prices over 5-minute processing-time tumbling windows using RocksDB's native MergeOperator for high-throughput state updates.

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [Architecture](#architecture)
- [Event Schemas](#event-schemas)
- [Processing Logic](#processing-logic)
- [Price Aggregation — RocksDB MergeOperator Design](#price-aggregation--rocksdb-mergeoperator-design)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Building](#building)
- [Running Locally](#running-locally)
- [Configuration](#configuration)
- [Further Reading](#further-reading)

---

## Problem Statement

An e-commerce platform produces clickstream events for each user action (page views, add-to-cart, checkout, etc.). Events are keyed by `(user_id, session_id)` and may arrive at the processing layer **up to 5 minutes late** relative to their true event time.

The goal is to detect the **last ≤ 5 events leading up to a `Checkout` event**, where the entire sequence falls within a **1-minute event-time window**, and emit a structured `CheckoutSession` record for downstream analytics.

Key constraints:

| Constraint | Value |
|---|---|
| Maximum out-of-order delay | 5 minutes |
| Checkout detection window | 1 minute (event-time) |
| Sequence length | Last ≤ 5 events before Checkout |
| Output latency target | As soon as the window is provably settled |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Kafka topic: clickstream                                        │
│  (JSON, keyed by user_id + "_" + session_id)                    │
└───────────────────────────┬──────────────────────────────────────┘
                            │ KafkaSource (SimpleStringSchema)
                            ▼
                   ClickStreamParser
                   (FlatMapFunction)
                   • Parse JSON → ClickStream POJO
                   • Extract optional properties.price
                   • Drop records with missing/invalid fields
                            │
                            ▼
              .keyBy(userId + "_" + sessionId)
                            │
                            ▼
              ClickStreamProcessFunction
              (KeyedProcessFunction)
              • Sorted per-key event buffer
              • Part 1: event-time flush when span > 6 min
              • Part 2: wall-clock idle flush after 5 min
              • Copies price from Checkout event into session
              • Emits CheckoutSession on detection
                            │
               ┌────────────┴────────────┐
               │                         │
               ▼                         ▼
  CheckoutSessionSerializer    .keyBy(_ → "global")
  • Serialize → JSON                     │
               │               PriceStatsWindowFunction
               │               (KeyedProcessFunction)
               │               • 5-min processing-time window
               │               • ReducingState → min, max price
               │                 (RocksDBReducingMergeState)
               │               • AggregatingState → avg price
               │                 (RocksDBAggregatingMergeState)
               │               • Emits PriceStats at window end
               │                         │
               │               PriceStatsSerializer
               │               • Serialize → JSON
               │                         │
  KafkaSink (idempotent)      KafkaSink (idempotent)
               │                         │
               ▼                         ▼
┌──────────────────────┐   ┌─────────────────────────────────┐
│  checkout-session    │   │  price-stats                    │
│  (CheckoutSession    │   │  (PriceStats per 5-min window)  │
│   JSON records)      │   └─────────────────────────────────┘
└──────────────────────┘
```

The local development stack (managed by `podman-compose`) adds:

- **Apache Kafka** (KRaft mode, no Zookeeper) — internal listener `kafka:9092`, external `localhost:9094`
- **Kafka UI** — browse topics and messages at `http://localhost:8080`
- **Flink JobManager / TaskManager** — Flink Web UI at `http://localhost:8081`

---

## Event Schemas

### Input — `clickstream` topic

Each message is a JSON object. The four top-level fields are required; `properties.price` is optional and read when present.

```json
{
  "user_id":    1042,
  "session_id": "a3f8c2d1-...",
  "event_time": "15/03/2025 14:22:07.483201",
  "event_name": "Checkout",
  "properties": {
    "price": 49.99
  },
  "context": { ... }
}
```

| Field | Type | Format | Required |
|---|---|---|---|
| `user_id` | integer | — | yes |
| `session_id` | string | UUID | yes |
| `event_time` | string | `dd/MM/yyyy HH:mm:ss.SSSSSS` | yes |
| `event_name` | string | e.g. `HomePage`, `Checkout` | yes |
| `properties.price` | number | decimal | no |

Records with a missing, blank, or unparseable required field are silently dropped by `ClickStreamParser`. A missing `properties.price` is treated as absent — the event is still processed, and the resulting `CheckoutSession` will have a `null` price.

### Output — `checkout-session` topic

```json
{
  "user_id":     1042,
  "session_id":  "a3f8c2d1-...",
  "start_time":  "15/03/2025 14:21:30.112000",
  "end_time":    "15/03/2025 14:22:07.483201",
  "duration":    37,
  "event_names": ["ProductPage", "AddToCart", "Checkout"],
  "price":       49.99
}
```

| Field | Type | Description |
|---|---|---|
| `user_id` | integer | User identifier |
| `session_id` | string | Session identifier |
| `start_time` | string | `event_time` of the first event in the detected sequence |
| `end_time` | string | `event_time` of the `Checkout` event |
| `duration` | long | `end_time − start_time` in seconds |
| `event_names` | string array | Ordered names of events in the sequence (≤ 5, ends with `Checkout`) |
| `price` | number \| null | Price from the `Checkout` event's `properties.price`; absent when not set |

### Output — `price-stats` topic

One record is emitted per 5-minute processing-time tumbling window, aggregated globally across all users and sessions.

```json
{
  "window_start": 1741650000000,
  "window_end":   1741650300000,
  "min_price":    9.99,
  "max_price":    199.00,
  "avg_price":    54.32,
  "count":        42
}
```

| Field | Type | Description |
|---|---|---|
| `window_start` | long | Epoch-ms of the inclusive window start |
| `window_end` | long | Epoch-ms of the exclusive window end |
| `min_price` | number | Minimum checkout price in the window |
| `max_price` | number | Maximum checkout price in the window |
| `avg_price` | number | Mean checkout price in the window |
| `count` | long | Number of priced checkout events in the window |

Windows containing no priced checkouts produce no output.

---

## Processing Logic

The core operator is `ClickStreamProcessFunction`, a `KeyedProcessFunction` with the following design:

### State

| State | Type | Purpose |
|---|---|---|
| `bufferState` | `ValueState<ArrayList<ClickStream>>` | Sorted (ascending event-time) buffer of received events |
| `maxEventTimeReceivedState` | `ValueState<Long>` | Wall-clock time when the latest-event-time event was received; used for idle detection |
| `currentTimerState` | `ValueState<Long>` | Timestamp of the active processing-time timer |

`minET` and `maxET` are derived directly from `buffer.get(0)` and `buffer.get(buffer.size()-1)` respectively; they are not stored separately. All state has a 10-minute TTL for automatic cleanup of abandoned sessions.

### Part 1 — Event-time flush

Triggered when `maxET − minET > 6 minutes` (= 5-min max lateness + 1-min checkout window). At this point, the first minute of the buffer is provably settled — no late event can still affect it.

The buffer is scanned left-to-right with a sliding window of ≤ 5 events:

- **Checkout found within 1 minute of `minET`** → emit `CheckoutSession`, remove the processed prefix, continue the loop.
- **No qualifying Checkout** → the first event (`buffer.get(0)`) is confirmed irrelevant and is dropped. `minET` advances to the next event and the loop continues.

This ensures the buffer is **continuously drained** down to at most 6 minutes of event-time, which bounds memory usage even during high-throughput historical replay from Kafka.

### Part 2 — Idle flush

A 30-second processing-time timer runs for every active key. On each tick, if `maxEventTimeReceivedState < now − 5 minutes`, the key is considered idle. A best-effort scan is performed to find any remaining `Checkout` event; if found it is emitted, then the entire buffer is discarded.

Part 2 handles sessions that never accumulate a 6-minute event-time span (e.g. a complete short session) and ensures buffers are eventually reclaimed in steady-state operation.

### Why not use Flink's built-in windows?

See [WINDOWING_ANALYSIS.md](WINDOWING_ANALYSIS.md) for a detailed comparison of tumbling, sliding, and session windows against the `KeyedProcessFunction` approach across interactivity, memory usage, events dropped, and checkout sessions detected.

---

## Price Aggregation — RocksDB MergeOperator Design

`PriceStatsWindowFunction` implements a 5-minute processing-time tumbling window as a `KeyedProcessFunction`. All `CheckoutSession` records are re-keyed to the constant `"global"` so that min/max/average are computed across the entire stream.

### The problem with naive read-modify-write state

With the default `ValueState`, every incoming event requires a full **read → deserialize → update → serialize → write** round-trip to RocksDB. For high-cardinality aggregations under sustained load this creates a bottleneck: each write is a point update that forces a full value rewrite into the LSM-tree, and every compaction must merge duplicates on the write path.

### How FRocksDB's MergeOperator eliminates the read

[FRocksDB](https://github.com/ververica/frocksdb) — the Flink-maintained fork of RocksDB — supports RocksDB's native **`AssociativeMergeOperator`** interface. Rather than reading the current value and writing back a new one, a merge operation appends a *merge operand* directly to the LSM-tree write path:

```
write path:  Merge(key, operand)  →  WAL  →  MemTable   (O(1), no read)
read path:   Get(key)             →  combine base value + all operands lazily
compaction:  operands are folded into the base value in the background
```

This means state updates are **pure appends** — no read is required on the hot write path. The actual combining work is deferred to reads and background compaction, which is exactly the right trade-off for a windowed aggregation that writes many values per window but reads only once (at the timer).

### How Flink exposes the MergeOperator

When the **RocksDB state backend** is active, Flink automatically provisions the efficient merge-backed state types in place of the generic `ValueState`:

| Flink state descriptor | RocksDB state type (internal) | Merge semantics |
|---|---|---|
| `ReducingStateDescriptor(Math::min)` | `RocksDBReducingMergeState` | Each `add(v)` call appends `v` as a merge operand; the `ReduceFunction` is used as the associative combiner |
| `ReducingStateDescriptor(Math::max)` | `RocksDBReducingMergeState` | Same pattern; combiner is `Math::max` |
| `AggregatingStateDescriptor(AvgAggregateFunction)` | `RocksDBAggregatingMergeState` | Each `add(v)` appends `v` as an operand; `AggregateFunction.merge` is the combiner used during compaction and reads |

No code changes are needed to opt in — registering the right descriptor with a RocksDB-backed environment is sufficient.

### State layout in `PriceStatsWindowFunction`

```
Key: "global"  (all sessions aggregated together)

minPriceState  ──  ReducingState<Double>
                   ReduceFunction: Math::min
                   → RocksDBReducingMergeState: each add() is a RocksDB Merge call

maxPriceState  ──  ReducingState<Double>
                   ReduceFunction: Math::max
                   → RocksDBReducingMergeState: each add() is a RocksDB Merge call

avgPriceState  ──  AggregatingState<Double, AvgAccumulator, Double>
                   AggregateFunction: AvgAggregateFunction
                   Accumulator: { sum: double, count: long }
                   merge(): new AvgAccumulator(a.sum + b.sum, a.count + b.count)
                   → RocksDBAggregatingMergeState: each add() is a RocksDB Merge call

countState     ──  ReducingState<Long>
                   ReduceFunction: Long::sum
                   → RocksDBReducingMergeState: each add(1L) is a RocksDB Merge call

windowEndState ──  ValueState<Long>
                   Epoch-ms of the registered processing-time timer
                   (read/written once per window; standard ValueState is fine here)
```

### Window lifecycle

```
  event arrives with price
          │
          ▼
  minPriceState.add(price)   ← RocksDB Merge (append-only, no read)
  maxPriceState.add(price)   ← RocksDB Merge (append-only, no read)
  avgPriceState.add(price)   ← RocksDB Merge (append-only, no read)
  countState.add(1L)         ← RocksDB Merge (append-only, no read)
          │
          ▼
  if windowEndState is null:
    register processing-time timer at next 5-min boundary
    store timer timestamp in windowEndState
          │
         ...  (more events arrive, each appending merge operands)
          │
          ▼
  processing-time timer fires at window boundary
          │
          ▼
  min   = minPriceState.get()   ← single RocksDB read + lazy merge fold
  max   = maxPriceState.get()   ← single RocksDB read + lazy merge fold
  avg   = avgPriceState.get()   ← single RocksDB read + lazy merge fold
  count = countState.get()      ← single RocksDB read + lazy merge fold
          │
          ▼
  emit PriceStats { window_start, window_end, min, max, avg, count }
          │
          ▼
  clear all state  ← next window starts fresh
```

The merge operands accumulated throughout the window are folded into the final value exactly **once per window** at read time, rather than on every incoming event. For a window receiving thousands of `CheckoutSession` records per minute, this substantially reduces RocksDB write amplification and CPU cost compared to a `ValueState`-based implementation.

---

## Project Structure

```
flink_streaming_event_reordering/
├── pom.xml                              # Maven build; Flink 2.2.0, Kafka connector 4.0.1-2.0
├── Makefile                             # All dev workflow targets (see make help)
├── podman-compose.yml                   # Local stack: Kafka + Flink + Kafka UI
├── WINDOWING_ANALYSIS.md                # Windowing strategy trade-off analysis
│
├── scripts/
│   ├── producer.py                      # Faker-based clickstream event generator
│   └── requirements.txt                 # faker, kafka-python
│
└── src/main/java/com/example/clickstream/
    ├── ClickStreamJob.java              # Main entry point; wires both Kafka source/sinks
    ├── model/
    │   ├── ClickStream.java             # Input POJO; includes optional price field
    │   ├── CheckoutSession.java         # Checkout output POJO; carries price from Checkout event
    │   └── PriceStats.java             # Price aggregation output POJO (min/max/avg per window)
    └── function/
        ├── ClickStreamParser.java       # FlatMapFunction: JSON → ClickStream; reads properties.price
        ├── ClickStreamProcessFunction.java  # KeyedProcessFunction: reorder + checkout detection
        └── PriceStatsWindowFunction.java    # KeyedProcessFunction: 5-min tumbling window price stats
                                             # Uses ReducingState (RocksDBReducingMergeState) for min/max
                                             # Uses AggregatingState (RocksDBAggregatingMergeState) for avg
```

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Java | 11 or 17 | `JAVA_HOME` must be set |
| Maven | 3.8+ | Used to build the fat JAR |
| Podman | 4.x+ | Container runtime for the local stack |
| podman-compose | 1.x | `pip install podman-compose` |
| Python | 3.9+ | For the event producer script |
| curl | any | Used by Makefile to call the Flink REST API |

> **Apple Silicon (ARM64):** all container images (`apache/kafka:latest`, `apache/flink:2.2-java11`, `provectuslabs/kafka-ui`) have native `linux/arm64` builds. No QEMU emulation is required.

---

## Building

```bash
make build
```

This runs `mvn package -DskipTests` and produces:

```
target/clickstream-event-reordering-1.0-SNAPSHOT.jar   (~57 MB fat JAR)
```

---

## Running Locally

All steps below are driven by the `Makefile`. Run `make help` for the full target list.

### 1. Start the local stack

```bash
make setup
```

This starts Kafka, Kafka UI, and the Flink cluster, waits for each service to become ready, and creates the `clickstream`, `checkout-session`, and `price-stats` topics. On success:

```
==> Setup complete!
    Kafka UI  : http://localhost:8080
    Flink UI  : http://localhost:8081
    Next step : make build && make submit-job && make produce
```

### 2. Build and submit the job

```bash
make build
make submit-job
```

`submit-job` uploads the fat JAR to the Flink REST API and starts the job with parallelism 2. The job appears in the Flink Web UI at `http://localhost:8081`.

### 3. Produce test events

```bash
make produce N=200
```

The Python producer generates `N` events across multiple simulated sessions and publishes them to the `clickstream` topic via the external Kafka listener (`localhost:9094`). Each session has two phases:

- **Phase 1 (≤ 55 s):** a checkout funnel or an abandoned browse sequence.
- **Phase 2 (7–15 min later):** additional events for the same session that push the event-time span past 6 minutes, triggering Part-1 processing in Flink.

Producer knobs (all overridable on the command line):

| Variable | Default | Description |
|---|---|---|
| `N` | 100 | Total number of events to produce |
| `CHECKOUT_RATIO` | 0.7 | Fraction of sessions that include a `Checkout` event |
| `LATE_RATIO` | 0.3 | Fraction of events sent out-of-order |
| `MAX_LATE_SECS` | 300 | Maximum arrival delay for out-of-order events (seconds) |

Example with custom settings:

```bash
make produce N=500 CHECKOUT_RATIO=0.9 LATE_RATIO=0.5
```

### 4. Observe output

```bash
make consume                # tail checkout-session topic (Ctrl-C to exit)
make consume-price-stats    # tail price-stats topic (Ctrl-C to exit)
```

Checkout session output (one record per line):

```
CreateTime:1741650127483  null | {"user_id":1042,"session_id":"a3f8c2d1-...","start_time":"...","end_time":"...","duration":37,"event_names":["ProductPage","AddToCart","Checkout"],"price":49.99}
```

Price stats output (one record emitted per 5-minute window):

```
CreateTime:1741650300000  null | {"window_start":1741650000000,"window_end":1741650300000,"min_price":9.99,"max_price":199.00,"avg_price":54.32,"count":42}
```

Browse events visually in the Kafka UI at `http://localhost:8080`.

### 5. Inspect logs

```bash
make logs-jm          # JobManager logs (shows buffer lifecycle and checkout emissions)
make logs-tm          # TaskManager logs
```

Look for log lines prefixed `Buffer CREATED`, `Buffer DESTROYED`, and `Emitting checkout session`.

### 6. Tear down

```bash
make teardown         # stop containers and wipe all volumes
```

---

## Configuration

The Flink job reads all Kafka configuration from environment variables. These are pre-set in `podman-compose.yml` for local development; override them when deploying to a real cluster.

| Environment variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_INPUT_TOPIC` | `clickstream` | Source topic |
| `KAFKA_OUTPUT_TOPIC` | `checkout-session` | Checkout session sink topic |
| `KAFKA_PRICE_STATS_TOPIC` | `price-stats` | Price aggregation sink topic |
| `KAFKA_CONSUMER_GROUP` | `clickstream-reordering` | Consumer group ID |

### Key constants in `ClickStreamProcessFunction`

These are compile-time constants; change them in the source and rebuild if you need different behaviour.

| Constant | Value | Description |
|---|---|---|
| `MAX_LATENESS_MS` | 5 min | Maximum expected out-of-order delay |
| `CHECKOUT_WINDOW_MS` | 1 min | Checkout must fall within this window of the sequence start |
| `TRIGGER_SPAN_MS` | 6 min | `MAX_LATENESS_MS + CHECKOUT_WINDOW_MS`; minimum buffer span before Part-1 fires |
| `IDLE_FLUSH_MS` | 5 min | Wall-clock idle time before Part-2 best-effort flush |
| `MAX_WINDOW_SIZE` | 5 | Maximum events tracked before a Checkout |
| `STATE_TTL_MINUTES` | 10 min | Flink state TTL for abandoned session cleanup |
| `TIMER_INTERVAL_MS` | 30 s | Processing-time timer interval for idle detection |

---

## Further Reading

- [WINDOWING_ANALYSIS.md](WINDOWING_ANALYSIS.md) — Detailed comparison of Flink windowing alternatives (tumbling, sliding, session) versus the custom `KeyedProcessFunction` approach.
- [Apache Flink 2.2 Documentation](https://nightlies.apache.org/flink/flink-docs-release-2.2/)
- [Flink Kafka Connector 4.x](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/)
- [FRocksDB — Flink's RocksDB fork](https://github.com/ververica/frocksdb) — Source for `RocksDBReducingMergeState` and `RocksDBAggregatingMergeState`; contains the `AssociativeMergeOperator` implementations wired up by Flink's state backend.
- [RocksDB Merge Operator wiki](https://github.com/facebook/rocksdb/wiki/Merge-Operator) — Explains the read/write/compaction mechanics that make merge-backed state updates cheaper than read-modify-write.
- [Flink Managed State docs](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/state/) — `ReducingState`, `AggregatingState`, and state backend configuration.
