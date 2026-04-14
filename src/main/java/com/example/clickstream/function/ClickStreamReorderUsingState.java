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
import java.util.Collections;
import java.util.Comparator;
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
public class ClickStreamReorderUsingState
        extends KeyedProcessFunction<String, ClickStream, CheckoutSession> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamReorderUsingState.class);

    // ------------------------------------------------------------------ constants

    /** State TTL – entries idle longer than this are garbage-collected. */
    private static final long STATE_TTL_MINUTES = ClickStreamReorderUsingHeap.STATE_TTL_MINUTES;

    /** Comparator that orders {@link ClickStream} events by ascending event-time. */
    private static final Comparator<ClickStream> BY_EVENT_TIME = ClickStreamReorderUsingHeap.BY_EVENT_TIME;
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
        ClickStreamReorderUsingHeap.insertSorted(buffer, event);
        // LOG.info("SK: Buffer length={} key={}", buffer.size(), currentKey);

        // ---- persist updated state before calling process() ----
        bufferState.update(buffer);
        maxEventTimeReceivedState.update(maxETR);

        // ---- run the core processing logic ----
        // process(out);

        // ---- re-read buffer after process() may have shrunk it ----
        if (buffer.isEmpty()) {
            clearAllState(ctx);
        } else if (currentTimerState.value() == null) {
            // Set timer the first time, the ongoing should be set from onTimer method.
            ClickStreamReorderUsingHeap.rescheduleTimer(ctx, currentTimerState, now);
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
        ArrayList<ClickStream> buffer = bufferState.value();
        int before = buffer == null ? -1 : buffer.size();
        ClickStreamReorderUsingHeap.process(maxEventTimeReceivedState.value(), currentKey, buffer, out);
        if (buffer != null && !buffer.isEmpty() && before != buffer.size()) {
            LOG.info("SK: setAcc buffer size: {} for key: {}", buffer.size(), currentKey);
            bufferState.update(buffer);
        }

        // ---- decide whether to re-schedule or clean up ----
        if (buffer == null || buffer.isEmpty()) {
            clearAllState(ctx);
        } else {
            ClickStreamReorderUsingHeap.rescheduleTimer(ctx, currentTimerState, System.currentTimeMillis());
        }
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
