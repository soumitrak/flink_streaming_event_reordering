# Windowing Strategy Analysis: Checkout Session Detection

## Problem Constraints

| Constraint | Value |
|---|---|
| Max out-of-order event delay | 5 minutes |
| Checkout window | ≤ 1 minute from the first event in the sequence |
| Sequence size | Last ≤ 5 events leading to "Checkout" |
| Late arrivals definition | An event whose event-time < (current max event-time − 5 min) |

The core challenge is that **the checkout sequence can arrive interleaved with, and delayed relative to, other events**. The correct set of "last ≤ 5 events before Checkout" is only knowable once we are confident no earlier event can still arrive.

---

## 1. Tumbling Window

### How it would work
Assign each event to a fixed, non-overlapping bucket (e.g., 1-minute tumbling windows in event-time). When the window closes, scan for a Checkout event and collect the preceding ≤ 5 events within the window.

### The boundary problem
A checkout funnel might look like:
```
Homepage  00:59   → window [00:00–01:00)
Checkout  01:01   → window [01:00–02:00)
```
The Checkout and its preceding events land in **different windows**. The window function sees neither a Checkout in the first window nor the lead-up events in the second. The session is silently discarded.

Widening the window to, say, 6 minutes (1-min checkout window + 5-min late tolerance) pushes the boundary problem out but does not eliminate it: any sequence that straddles a 6-minute boundary is still lost.

### Allowed lateness overhead
To handle 5-minute late arrivals the window must stay open for `window_size + 5 min`. A 1-minute window that must tolerate 5-minute late events keeps **6× the per-key memory** a naive implementation would use, because all six 1-minute windows are alive simultaneously per key.

### Summary

| Dimension | Assessment |
|---|---|
| **Interactivity** | Low – result fires only after `window_size + lateness` elapses |
| **Memory** | Moderate – one window's events per key, but replicated `lateness/window_size` times |
| **Events dropped** | **High** – any sequence crossing a window boundary is entirely lost |
| **Checkout sessions found** | **Lowest** – fundamental boundary split problem means the majority of real checkout sequences can be missed |

---

## 2. Sliding Window

### How it would work
Use a 1-minute window that slides every, say, 10 seconds. Each event is placed into `window_size / slide` = 6 windows simultaneously.

### Duplicate emission
Because windows overlap, the same checkout sequence will be detected and emitted by multiple windows. A deduplification step (e.g., keyed on `userId + sessionId + checkout event-time`) is required downstream, which adds operational complexity.

### Memory explosion
With a 1-minute window sliding every 10 seconds, each event is stored in **6 copies** in Flink state. Add 5-minute allowed lateness, and every sliding window stays open for `1 min + 5 min = 6 min`, with a new window starting every 10 seconds. That means **36 concurrent open windows per key**, each holding its own copy of events. Memory usage is the highest of all approaches by a wide margin.

### Still misses cross-boundary sequences
Sliding reduces the probability of a boundary miss compared to tumbling, but does not eliminate it. A checkout sequence that spans > 1 minute (the window size) will still be split: no single window captures all the lead-up events and the Checkout together.

### Summary

| Dimension | Assessment |
|---|---|
| **Interactivity** | Moderate – fires every `slide` interval, but with lateness delay |
| **Memory** | **Highest** – `window_size/slide × lateness_windows` copies of every event per key |
| **Events dropped** | Moderate – reduced boundary problem vs tumbling, but sequences longer than the window are still split |
| **Checkout sessions found** | Moderate (raw count inflated by duplicates; after dedup, similar to tumbling) |

---

## 3. Session Window

### How it would work
Group events into sessions separated by an inactivity gap (e.g., 5 minutes, matching the maximum late-arrival delay). Flink automatically merges session state as events arrive and fires the window function when the gap expires.

### Naturally models user behaviour
A session window with a 5-minute gap is the closest built-in primitive to this use case: it accumulates all events for a `(userId, sessionId)` key until the user has been idle for 5 minutes, then emits the whole session.

### Still requires custom logic inside the window
The window function receives the **entire session** as a collection of events — potentially hundreds — and must itself implement the "find the last ≤ 5 events before each Checkout" scan. All the detection logic must still be written; the session window only handles the lifetime of the buffer.

### Output delay
A session fires only after the inactivity gap expires. With a 5-minute gap, every checkout result is delayed by **at least 5 minutes of wall-clock time** after the last event in the session. For fast-moving funnels (user checks out in 30 seconds) this means waiting nearly 5 minutes for what could have been emitted seconds after the Checkout event arrived.

### Multiple checkouts in one session
If a user performs two separate checkout funnels within the same session window, the window function must find and emit two `CheckoutSession` records. This is possible but not handled automatically; it requires the same sliding-scan logic that the custom `KeyedProcessFunction` implements.

### Late-event retractions
If a late event arrives after the session fired (within the allowed lateness period), Flink can re-fire the window. Without a retraction mechanism the consumer sees duplicate output records for the same session; with retractions enabled the downstream system must handle `(retract, emit)` pairs.

### Memory
The entire session is buffered in managed Flink state until the gap expires. For a user with a long browsing session (30 minutes of events before a checkout) every event is held in memory until 5 minutes after the last one — substantially more than the custom approach, which trims the buffer as soon as a checkout sequence is confirmed.

### Summary

| Dimension | Assessment |
|---|---|
| **Interactivity** | **Low** – minimum 5-minute (gap) + up to 5-minute (lateness) = up to 10-minute output delay |
| **Memory** | Moderate-to-high – entire session buffered until gap fires; no trimming until window closes |
| **Events dropped** | Low – all events within the gap are captured, including out-of-order ones within the allowed lateness |
| **Checkout sessions found** | Good – captures full sessions, but requires custom scan inside the window function; same detection quality as the custom approach, but with higher latency |

---

## 4. Custom KeyedProcessFunction (current implementation)

### How it works
Events are inserted into a per-key sorted buffer. Two flush paths exist:

- **Part 1 (event-time flush):** When `maxET − minET > 6 min`, the oldest minute of the buffer is "settled" (all late arrivals for that minute have had time to arrive). The buffer is scanned for Checkout events; matching sessions are emitted and the consumed prefix is trimmed.
- **Part 2 (idle flush):** When no event with a higher event-time has arrived for 5 minutes of wall-clock time (`maxEventTimeReceivedState`), a best-effort scan is performed and the buffer is discarded.

### Interactivity
Part 1 fires as soon as the event-time span exceeds 6 minutes, which can happen well before the session ends. A checkout that occurs at T+45 seconds followed by post-session events at T+7 minutes will be detected and emitted at processing-time ≈ T+7 minutes, not T+12 minutes (what session window would require).

### Memory
The buffer is **trimmed continuously** during Part 1, not just when a checkout is confirmed. When the span condition (`maxET − minET > 6 min`) is met but no qualifying Checkout exists within the first minute of the buffer, the oldest event is dropped unconditionally — it is fully settled and can never be part of a future checkout window. This loop repeats until either a Checkout is found and emitted, or the buffer shrinks below the 6-minute span threshold.

The result is a hard bound: **the buffer never accumulates more than 6 minutes of event-time per key**, regardless of how fast events arrive. This is the critical property that makes the implementation safe under high-throughput historical replay from Kafka. All other approaches (tumbling, sliding, session) hold their entire window or session in state until the window closes; this implementation continuously discards events the moment they are confirmed irrelevant.

For sessions with a confirmed checkout, the buffer is additionally trimmed by removing the processed prefix (events up to and including the Checkout). Only the unconsumed tail — events after the last emitted Checkout — remains in state.

### Events dropped
The only events that can be missed are those that arrive after Part 2's idle flush has discarded the buffer. This requires an event to be more than 5 minutes late on the wall clock *and* arrive after the buffer has already been cleared — a rare edge case.

### Checkout sessions found
Best among all approaches. The combination of event-time-driven (Part 1) and wall-clock-driven (Part 2) flushing means every qualifying checkout sequence is detected regardless of session length, interleaved events, or out-of-order arrivals within the 5-minute tolerance.

### Downsides
- Most complex to implement and maintain.
- Relies on a heuristic (6-minute span) rather than true watermarks, so correctness depends on the assumed maximum delay of 5 minutes being honoured by producers.
- The processing-time timer approach means the 30-second timer tick is the minimum latency for Part 2 to act; a watermark-driven approach would be more precise.
- No built-in Flink UI metric for "window lag" — custom monitoring is needed.

### Summary

| Dimension | Assessment |
|---|---|
| **Interactivity** | **Best** – Part 1 fires as soon as event-time span > 6 min, without waiting for session end |
| **Memory** | **Lowest** – buffer is trimmed after each confirmed checkout; only unconsumed tail is retained |
| **Events dropped** | **Lowest** – only events arriving after a wall-clock idle flush are lost |
| **Checkout sessions found** | **Highest** – both flush paths together capture every qualifying sequence |

---

## Consolidated Comparison

| | Tumbling | Sliding | Session | KeyedProcessFunction |
|---|---|---|---|---|
| **Checkout sessions found** | Lowest | Moderate (with duplicates) | Good | **Best** |
| **Events dropped** | High | Moderate | Low | **Lowest** |
| **Memory per key** | Moderate | **Highest** | Moderate–High | **Lowest** (hard-bounded to 6 min of event-time) |
| **Output latency** | High | Moderate | **Highest** (gap + lateness) | **Lowest** |
| **Implementation complexity** | Low | Low | Moderate | High |
| **Duplicate output risk** | None | **High** | Moderate (with late firings) | None |
| **Handles multi-checkout sessions** | No | No (dedup required) | Requires custom scan | **Yes, natively** |
| **Boundary-split problem** | Severe | Moderate | None | None |

---

## Conclusion

The built-in windowing primitives are poor fits for this problem for three related reasons:

1. **Tumbling and sliding windows have a fixed-boundary problem.** A checkout funnel that straddles a window boundary is silently dropped or duplicated. No amount of lateness allowance eliminates this; it only reduces the probability.

2. **Session windows eliminate the boundary problem but maximise output latency.** Every result is delayed by the full inactivity gap (5 min) plus allowed lateness (5 min), and the entire session must be held in memory until the gap fires.

3. **None of the built-in windows trim their buffers early.** The `KeyedProcessFunction` continuously drops events from the front of the buffer the moment they are confirmed settled and unable to join a future checkout window. This gives the buffer a hard ceiling of 6 minutes of event-time per key — a bound that holds even during high-throughput historical replay from Kafka, where wall-clock-based idle detection (Part 2) would not fire for minutes. All built-in window types accumulate their full contents until the window closes.

The custom `KeyedProcessFunction` is the right choice here precisely because the detection criterion ("last ≤ 5 events before Checkout within 1 minute") is a **sequence pattern within a stream**, not a time-bucketing problem. Windowing is designed for aggregations over time buckets; sequence pattern detection over a bounded history requires the finer-grained control that `KeyedProcessFunction` provides.
