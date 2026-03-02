package com.example.clickstream.function;

import com.example.clickstream.model.CheckoutSession;
import com.example.clickstream.model.ClickStream;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Custom {@link KeyedProcessFunction} that:
 * <ol>
 *   <li>Buffers incoming {@link ClickStream} events in a sorted (by event-time) ArrayList.</li>
 *   <li>Re-orders out-of-order events (late arrivals up to 5 min are expected).</li>
 *   <li>Detects the last ≤5 events leading up to a "Checkout" event within a 1-minute window.</li>
 *   <li>Emits a {@link CheckoutSession} for every qualifying checkout sequence found.</li>
 * </ol>
 *
 * <p>State TTL is set to 10 minutes so that abandoned sessions are eventually cleaned up.
 *
 * <p>Timers are processing-time timers fired every 30 seconds so that the flush logic
 * runs even when no new events arrive for a key.
 */
public class ClickStreamProcessFunction
        extends KeyedProcessFunction<String, ClickStream, CheckoutSession> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamProcessFunction.class);

    // ------------------------------------------------------------------ constants

    /** State TTL – entries idle longer than this are garbage-collected. */
    private static final long STATE_TTL_MINUTES = 10L;

    /** Maximum late arrival time. */
    private static final long MAX_LATENESS_MS = 5L * 60 * 1_000;

    /** The checkout-window width: a Checkout must occur within 1 minute of the window start. */
    private static final long CHECKOUT_WINDOW_MS = 1L * 60 * 1_000;

    /**
     * The minimum span (maxEventTime − minEventTime) that must exist in the buffer
     * before we are confident that the 1-minute checkout window is fully populated
     * (accounts for 5-min late arrivals + 1-min window = 6 min).
     */
    private static final long TRIGGER_SPAN_MS = MAX_LATENESS_MS + CHECKOUT_WINDOW_MS;

    /**
     * If no new event has been received for this long (wall-clock), flush the buffer
     * regardless of the event-time span.
     */
    private static final long IDLE_FLUSH_MS = MAX_LATENESS_MS;

    /** Maximum number of events tracked in the sliding window leading to a Checkout. */
    private static final int MAX_WINDOW_SIZE = 5;

    /** Interval between processing-time timers. */
    private static final long TIMER_INTERVAL_MS = 30L * 1_000;

    // ------------------------------------------------------------------ state descriptors

    /**
     * Sorted buffer of received events, keyed by (user_id, session_id).
     * Maintained in ascending event-time order.
     */
    private ValueState<ArrayList<ClickStream>> bufferState;

    /**
     * Wall-clock time (epoch ms) at which the event carrying {@code maxEventTime} was
     * received by this operator. Used to detect idle keys.
     */
    private ValueState<Long> maxEventTimeReceivedState;

    /**
     * Timestamp of the currently registered processing-time timer, so we can cancel
     * it before registering a new one.
     */
    private ValueState<Long> currentTimerState;

    /**
     * The key currently being processed.  Set at the top of {@link #processElement} and
     * {@link #onTimer} so that private helpers (which don't receive a Context) can log it.
     * Safe because Flink guarantees single-threaded, non-concurrent invocation per subtask.
     */
    private transient String currentKey;

    // ------------------------------------------------------------------ lifecycle

    @Override
    public void open(OpenContext parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofMinutes(STATE_TTL_MINUTES))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<ArrayList<ClickStream>> bufferDesc =
                new ValueStateDescriptor<>(
                        "buffer",
                        TypeInformation.of(new TypeHint<ArrayList<ClickStream>>() {}));
        bufferDesc.enableTimeToLive(ttlConfig);
        bufferState = getRuntimeContext().getState(bufferDesc);

        ValueStateDescriptor<Long> maxETRDesc = new ValueStateDescriptor<>("maxEventTimeReceived", Long.class);
        maxETRDesc.enableTimeToLive(ttlConfig);
        maxEventTimeReceivedState = getRuntimeContext().getState(maxETRDesc);

        ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("currentTimer", Long.class);
        currentTimerState = getRuntimeContext().getState(timerDesc);
    }

    // ------------------------------------------------------------------ processElement

    @Override
    public void processElement(ClickStream event,
                               Context ctx,
                               Collector<CheckoutSession> out) throws Exception {
        currentKey = ctx.getCurrentKey();

        // ---- read current state ----
        ArrayList<ClickStream> buffer = bufferState.value();
        if (buffer == null) {
            buffer = new ArrayList<>();
            LOG.info("Buffer CREATED key={}", currentKey);
        }

        Long maxETR = maxEventTimeReceivedState.value();

        long eventTimeMillis = event.getEventTimeMillis();
        long now = System.currentTimeMillis();

        // maxET is derived from the last element of the sorted buffer; update the
        // wall-clock receipt time whenever this event extends the maximum.
        if (buffer.isEmpty() || eventTimeMillis > buffer.get(buffer.size() - 1).getEventTimeMillis()) {
            maxETR = now;
        }

        // ---- insert event into the sorted buffer ----
        insertSorted(buffer, event);

        // ---- persist updated state before calling process() ----
        bufferState.update(buffer);
        maxEventTimeReceivedState.update(maxETR);

        // ---- run the core processing logic ----
        process(out);

        // ---- re-read buffer after process() may have shrunk it ----
        buffer = bufferState.value();
        if (buffer == null || buffer.isEmpty()) {
            clearAllState(ctx);
        } else {
            rescheduleTimer(ctx, now);
        }
    }

    // ------------------------------------------------------------------ onTimer

    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<CheckoutSession> out) throws Exception {
        currentKey = ctx.getCurrentKey();

        // The timer has fired – clear the timer tracking state.
        currentTimerState.clear();

        // ---- run the core processing logic ----
        process(out);

        // ---- decide whether to re-schedule or clean up ----
        ArrayList<ClickStream> buffer = bufferState.value();
        if (buffer == null || buffer.isEmpty()) {
            clearAllState(ctx);
        } else {
            rescheduleTimer(ctx, System.currentTimeMillis());
        }
    }

    // ------------------------------------------------------------------ process (core logic)

    /**
     * Core reordering and session-detection logic.
     *
     * <p><b>Part 1 – event-time driven flush:</b> While the buffer spans more than
     * {@value #TRIGGER_SPAN_MS} ms (6 minutes) the 1-minute checkout window is stable
     * enough to act on.  We scan left-to-right maintaining a sliding window of the last
     * {@value #MAX_WINDOW_SIZE} events.  When we encounter a "Checkout" event whose
     * event-time falls within 1 minute of the current {@code minEventTime} we emit a
     * {@link CheckoutSession} and remove the processed prefix from the buffer.
     * When no qualifying Checkout is found, the first event (at {@code minET}) is dropped
     * because it is fully settled and can never join a future checkout window; this keeps
     * the buffer bounded to at most {@value #TRIGGER_SPAN_MS} ms of event-time regardless
     * of how fast events arrive (fixing the high-throughput historical-replay memory issue).
     *
     * <p><b>Part 2 – idle/wall-clock driven flush:</b> If the key has been idle for more
     * than {@value #IDLE_FLUSH_MS} ms we perform a best-effort scan to find any Checkout
     * in the remaining buffer, emit it if found, and then discard all remaining events.
     */
    private void process(Collector<CheckoutSession> out) throws Exception {
        ArrayList<ClickStream> buffer = bufferState.value();
        if (buffer == null || buffer.isEmpty()) return;

        Long maxETR = maxEventTimeReceivedState.value();

        // minET and maxET are derived directly from the sorted buffer.
        long minET = buffer.get(0).getEventTimeMillis();
        long maxET = buffer.get(buffer.size() - 1).getEventTimeMillis();

        // ========================= Part 1 =========================
        // Keep flushing while the buffer spans > 6 minutes (meaning the first minute
        // of the window has been fully received, accounting for late arrivals).
        while (!buffer.isEmpty() && (maxET - minET) > TRIGGER_SPAN_MS) {

            // Slide a fixed-size window of MAX_WINDOW_SIZE across the buffer.
            // Stop as soon as a Checkout event is encountered.
            Deque<ClickStream> window = new ArrayDeque<>(MAX_WINDOW_SIZE);
            int checkoutIdx = -1;
            ClickStream checkoutEvent = null;

            for (int i = 0; i < buffer.size(); i++) {
                ClickStream e = buffer.get(i);
                if (window.size() >= MAX_WINDOW_SIZE) {
                    window.poll();  // evict the oldest to keep size ≤ MAX_WINDOW_SIZE
                }
                window.offer(e);
                if ("Checkout".equals(e.getEventName())) {
                    checkoutEvent = e;
                    checkoutIdx = i;
                    break;
                }
            }

            // Only emit when:
            //   (a) a Checkout was found in the scan, AND
            //   (b) the Checkout happened within the 1-minute checkout window.
            if (checkoutEvent != null
                    && checkoutEvent.getEventTimeMillis() < minET + CHECKOUT_WINDOW_MS) {

                CheckoutSession session = buildSession(new ArrayList<>(window));
                LOG.info("Emitting checkout session: userId={}, sessionId={}, events={}",
                        session.getUserId(), session.getSessionId(), session.getEventNames());
                out.collect(session);

                // Remove the processed prefix (inclusive of the Checkout event).
                buffer.subList(0, checkoutIdx + 1).clear();

                // Update minEventTime / maxEventTime / maxEventTimeReceived to
                // reflect the remaining buffer contents.
                if (!buffer.isEmpty()) {
                    // buffer is sorted: first element has min time, last has max time.
                    // maxETR is unchanged – the last element (max event-time) is the same.
                    minET = buffer.get(0).getEventTimeMillis();
                    maxET = buffer.get(buffer.size() - 1).getEventTimeMillis();
                } else {
                    // Buffer is now empty – clear wall-clock tracking state.
                    maxEventTimeReceivedState.clear();
                    break;
                }

            } else {
                // The first event in the buffer (at minET) is confirmed settled: the
                // 6-minute span guarantees no late arrival can still affect its window.
                // Either no Checkout exists within 1 minute of minET, or the first
                // Checkout found is too far in the future.  Either way, buffer.get(0)
                // can never be part of a qualifying session – drop it and advance minET.
                buffer.remove(0);
                if (buffer.isEmpty()) break;
                minET = buffer.get(0).getEventTimeMillis();
                maxET = buffer.get(buffer.size() - 1).getEventTimeMillis();
            }
        }

        // Persist the (potentially trimmed) buffer.
        bufferState.update(buffer);

        // ========================= Part 2 =========================
        // If no new event has arrived for IDLE_FLUSH_MS ms, perform a best-effort flush.
        if (!buffer.isEmpty()
                && maxETR != null
                && maxETR < System.currentTimeMillis() - IDLE_FLUSH_MS) {

            LOG.info("Key has been idle for > {} ms – performing best-effort flush. " +
                     "Buffer size: {}", IDLE_FLUSH_MS, buffer.size());

            Deque<ClickStream> window = new ArrayDeque<>(MAX_WINDOW_SIZE);
            ClickStream checkoutEvent = null;

            for (ClickStream e : buffer) {
                if (window.size() >= MAX_WINDOW_SIZE) {
                    window.poll();
                }
                window.offer(e);
                if ("Checkout".equals(e.getEventName())) {
                    checkoutEvent = e;
                    break;
                }
            }

            if (checkoutEvent != null) {
                CheckoutSession session = buildSession(new ArrayList<>(window));
                LOG.info("Emitting checkout session (idle flush): userId={}, sessionId={}",
                        session.getUserId(), session.getSessionId());
                out.collect(session);
            } else {
                LOG.info("No Checkout found during idle flush – discarding buffer.");
            }

            // Discard everything regardless of whether we emitted.
            buffer.clear();
            bufferState.update(buffer);
            maxEventTimeReceivedState.clear();
        }
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Inserts {@code event} into {@code buffer} maintaining ascending event-time order.
     * Uses binary search for O(log n) probe + O(n) shift.
     */
    private void insertSorted(ArrayList<ClickStream> buffer, ClickStream event) {
        int lo = 0;
        int hi = buffer.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (buffer.get(mid).getEventTimeMillis() <= event.getEventTimeMillis()) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        buffer.add(lo, event);
    }

    /**
     * Builds a {@link CheckoutSession} from the ordered list of events in the window.
     * The first event provides start_time/user_id/session_id; the last event (Checkout)
     * provides end_time.
     */
    private CheckoutSession buildSession(List<ClickStream> events) {
        ClickStream first   = events.get(0);
        ClickStream last    = events.get(events.size() - 1);   // Checkout event

        long durationSeconds =
                (last.getEventTimeMillis() - first.getEventTimeMillis()) / 1_000L;

        List<String> eventNames = events.stream()
                .map(ClickStream::getEventName)
                .collect(Collectors.toList());

        return new CheckoutSession(
                first.getUserId(),
                first.getSessionId(),
                first.getEventTime(),
                last.getEventTime(),
                durationSeconds,
                eventNames);
    }

    /**
     * Cancels the existing processing-time timer (if any) and registers a new one
     * {@value #TIMER_INTERVAL_MS} ms in the future.
     */
    private void rescheduleTimer(KeyedProcessFunction<String, ClickStream, CheckoutSession>.Context ctx,
                                 long now) throws Exception {
        Long existing = currentTimerState.value();
        if (existing != null) {
            ctx.timerService().deleteProcessingTimeTimer(existing);
        }
        long next = now + TIMER_INTERVAL_MS;
        ctx.timerService().registerProcessingTimeTimer(next);
        currentTimerState.update(next);
    }

    /**
     * Clears all state entries and cancels any outstanding timer for the current key.
     */
    private void clearAllState(KeyedProcessFunction<String, ClickStream, CheckoutSession>.Context ctx)
            throws Exception {
        LOG.info("Buffer DESTROYED key={}", currentKey);
        Long existing = currentTimerState.value();
        if (existing != null) {
            ctx.timerService().deleteProcessingTimeTimer(existing);
        }
        bufferState.clear();
        maxEventTimeReceivedState.clear();
        currentTimerState.clear();
    }
}
