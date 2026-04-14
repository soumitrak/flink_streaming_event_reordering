package com.example.clickstream.function;

import com.example.clickstream.model.CheckoutSession;
import com.example.clickstream.model.ClickStream;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
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
public class ClickStreamReorderUsingMergeState
        extends KeyedProcessFunction<String, ClickStream, CheckoutSession> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamReorderUsingMergeState.class);

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
    private AggregatingMergeState<ClickStream, SortedListAccumulator, ArrayList<ClickStream>> bufferState;

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
     * Number of events currently stored in {@code bufferState}.
     * Kept in sync with every insertion and removal so that empty/non-empty decisions
     * can be made without deserializing the full buffer via {@code bufferState.get()}.
     */
    private ValueState<Long> numBufferedEventsState;

    /**
     * Event-time (epoch ms) of the latest event currently held in {@code bufferState}.
     * Kept in sync so that {@link #processElement} can determine whether an incoming
     * event extends the maximum without fetching the full buffer.
     */
    private ValueState<Long> maxEventTimeState;

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

        AggregatingMergeStateDescriptor<ClickStream, SortedListAccumulator, ArrayList<ClickStream>> bufferDesc = new AggregatingMergeStateDescriptor<>(
                "buffer",
                new SortedListAggregator(),
                new SortedListAccumulatorTypeInfo());
        // bufferDesc.enableTimeToLive(ttlConfig); // SK: Fix it
        bufferState = getRuntimeContext().getAggregatingMergeState(bufferDesc);

        ValueStateDescriptor<Long> maxETRDesc = new ValueStateDescriptor<>("maxEventTimeReceived", Long.class);
        maxETRDesc.enableTimeToLive(ttlConfig);
        maxEventTimeReceivedState = getRuntimeContext().getState(maxETRDesc);

        ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("currentTimer", Long.class);
        currentTimerState = getRuntimeContext().getState(timerDesc);

        ValueStateDescriptor<Long> numBufferedDesc = new ValueStateDescriptor<>("numBufferedEvents", Long.class);
        numBufferedDesc.enableTimeToLive(ttlConfig);
        numBufferedEventsState = getRuntimeContext().getState(numBufferedDesc);

        ValueStateDescriptor<Long> maxEventTimeDesc = new ValueStateDescriptor<>("maxEventTime", Long.class);
        maxEventTimeDesc.enableTimeToLive(ttlConfig);
        maxEventTimeState = getRuntimeContext().getState(maxEventTimeDesc);
    }

    // ------------------------------------------------------------------ processElement

    @Override
    public void processElement(ClickStream event,
                               Context ctx,
                               Collector<CheckoutSession> out) throws Exception {
        currentKey = ctx.getCurrentKey();

        // ---- read current state ----
        Long numBuffered = numBufferedEventsState.value();
        Long maxETR = maxEventTimeReceivedState.value();

        long eventTimeMillis = event.getEventTimeMillis();
        long now = System.currentTimeMillis();

        if (numBuffered == null || numBuffered == 0L) {
            bufferState.setAcc(new SortedListAccumulator());
            LOG.info("SK: Buffer CREATED key={}", currentKey);
            numBuffered = 0L;
            // First event in the buffer is by definition the new maximum.
            maxETR = now;
            maxEventTimeState.update(eventTimeMillis);
        } else {
            // Compare against the tracked max event time to avoid a bufferState.get().
            Long maxET = maxEventTimeState.value();
            if (maxET == null || eventTimeMillis > maxET) {
                maxETR = now;
                maxEventTimeState.update(eventTimeMillis);
            }
        }

        // ---- insert event into the sorted buffer ----
        bufferState.add(event);
        numBufferedEventsState.update(numBuffered + 1L);

        // ---- persist updated state before calling process() ----
        maxEventTimeReceivedState.update(maxETR);

        // ---- run the core processing logic ----
        // process(out);

        // ---- decide whether to re-schedule or clean up using the event counter ----
        numBuffered = numBufferedEventsState.value();
        if (numBuffered == null || numBuffered == 0L) {
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
        long st = System.nanoTime();
        ArrayList<ClickStream> buffer = bufferState.get();
        long tt = System.nanoTime() - st;
        LOG.info("SK: Time taken in get call: {} ms size: {} for key: {}", tt/1000000.0, buffer.size(), currentKey);
        int before = buffer == null ? -1 : buffer.size();
        ClickStreamReorderUsingHeap.process(maxEventTimeReceivedState.value(), currentKey, buffer, out);
        if (buffer != null && !buffer.isEmpty()) {
            // Setting it irrespective of change to save the cost of merge in next get call
            LOG.info("SK: setAcc buffer size: {} for key: {}", buffer.size(), currentKey);
            bufferState.setAcc(new SortedListAccumulator(buffer));
        }
        if (buffer != null && !buffer.isEmpty() && before != buffer.size()) {
            // SK: Set is always in onTimer to save the cost of merge in next get call
            // bufferState.setAcc(new SortedListAccumulator(buffer));
            numBufferedEventsState.update((long) buffer.size());
            maxEventTimeState.update(buffer.get(buffer.size() - 1).getEventTimeMillis());
        }

        // ---- decide whether to re-schedule or clean up using the event counter ----
        if (buffer == null || buffer.isEmpty()) {
            clearAllState(ctx);
        } else if (currentTimerState.value() == null) {
            // Set timer the first time, the ongoing should be set from onTimer method.
            ClickStreamReorderUsingHeap.rescheduleTimer(ctx, currentTimerState, System.currentTimeMillis());
        }
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Inserts {@code event} into {@code buffer} maintaining ascending event-time order.
     * Uses binary search for O(log n) probe + O(n) shift.
     */
    private static void insertSorted(ArrayList<ClickStream> buffer, ClickStream event) {
        int idx = Collections.binarySearch(buffer, event, BY_EVENT_TIME);
        // binarySearch returns -(insertion point) - 1 when no match exists;
        // when a match exists, insert just after it to preserve arrival order among ties.
        int insertionPoint = (idx >= 0) ? idx + 1 : -(idx + 1);
        buffer.add(insertionPoint, event);
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
        numBufferedEventsState.clear();
        maxEventTimeState.clear();
        maxEventTimeReceivedState.clear();
        currentTimerState.clear();
    }

    public static class SortedListAccumulator implements Serializable {

        private static final long serialVersionUID = 1L;

        public final ArrayList<ClickStream> buffer;

        public SortedListAccumulator() {
            buffer = new ArrayList<>();
        }

        public SortedListAccumulator(ArrayList<ClickStream> buffer) {
            this.buffer = new ArrayList<>(buffer);
        }
    }

    public static class SortedListAggregator
            implements AggregateFunction<ClickStream, ClickStreamReorderUsingMergeState.SortedListAccumulator, ArrayList<ClickStream>> {

        private static final long serialVersionUID = 1L;

        @Override
        public ClickStreamReorderUsingMergeState.SortedListAccumulator createAccumulator() {
            return new ClickStreamReorderUsingMergeState.SortedListAccumulator();
        }

        @Override
        public ClickStreamReorderUsingMergeState.SortedListAccumulator add(ClickStream cs, ClickStreamReorderUsingMergeState.SortedListAccumulator acc) {
            insertSorted(acc.buffer, cs);
            return acc;
        }

        @Override
        public ArrayList<ClickStream> getResult(ClickStreamReorderUsingMergeState.SortedListAccumulator acc) {
            return acc.buffer;
        }

        @Override
        public ClickStreamReorderUsingMergeState.SortedListAccumulator merge(ClickStreamReorderUsingMergeState.SortedListAccumulator a, ClickStreamReorderUsingMergeState.SortedListAccumulator b) {
            ArrayList<ClickStream> list1 = a.buffer;
            ArrayList<ClickStream> list2 = b.buffer;
            ArrayList<ClickStream> merged = new ArrayList<>(list1.size() + list2.size());
        int i = 0, j = 0;

        // Traverse both lists and add the smaller element
        while (i < list1.size() && j < list2.size()) {
            if (BY_EVENT_TIME.compare(list1.get(i), list2.get(j)) < 0) {
                merged.add(list1.get(i++));
            } else {
                merged.add(list2.get(j++));
            }
        }

        // Append remaining elements from list1
        while (i < list1.size()) {
            merged.add(list1.get(i++));
        }

        // Append remaining elements from list2
        while (j < list2.size()) {
            merged.add(list2.get(j++));
        }

            return new ClickStreamReorderUsingMergeState.SortedListAccumulator(merged);
        }
    }

    /**
     * Efficient binary serializer for {@link SortedListAccumulator}.
     *
     * <p>Encodes each {@link ClickStream} field directly using primitive DataOutputView methods,
     * avoiding Kryo overhead and enabling proper Flink schema-evolution snapshots.
     */
    public static final class SortedListAccumulatorSerializer
            extends TypeSerializer<SortedListAccumulator> {

        private static final long serialVersionUID = 1L;

        public static final SortedListAccumulatorSerializer INSTANCE =
                new SortedListAccumulatorSerializer();

        private SortedListAccumulatorSerializer() {}

        // ---- TypeSerializer contract ----------------------------------------

        @Override public boolean isImmutableType() { return false; }

        @Override
        public TypeSerializer<SortedListAccumulator> duplicate() { return this; }

        @Override
        public SortedListAccumulator createInstance() { return new SortedListAccumulator(); }

        @Override
        public SortedListAccumulator copy(SortedListAccumulator from) {
            return new SortedListAccumulator(from.buffer);
        }

        @Override
        public SortedListAccumulator copy(SortedListAccumulator from, SortedListAccumulator reuse) {
            return copy(from);
        }

        @Override public int getLength() { return -1; }

        @Override
        public void serialize(SortedListAccumulator record, DataOutputView target)
                throws java.io.IOException {
            ArrayList<ClickStream> buf = record.buffer;
            target.writeInt(buf.size());
            for (ClickStream cs : buf) {
                serializeEvent(cs, target);
            }
        }

        @Override
        public SortedListAccumulator deserialize(DataInputView source)
                throws java.io.IOException {
            int size = source.readInt();
            ArrayList<ClickStream> buf = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                buf.add(deserializeEvent(source));
            }
            return new SortedListAccumulator(buf);
        }

        @Override
        public SortedListAccumulator deserialize(SortedListAccumulator reuse, DataInputView source)
                throws java.io.IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws java.io.IOException {
            int size = source.readInt();
            target.writeInt(size);
            for (int i = 0; i < size; i++) {
                serializeEvent(deserializeEvent(source), target);
            }
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof SortedListAccumulatorSerializer;
        }

        @Override
        public int hashCode() { return SortedListAccumulatorSerializer.class.hashCode(); }

        @Override
        public TypeSerializerSnapshot<SortedListAccumulator> snapshotConfiguration() {
            return new SerializerSnapshot();
        }

        /** Snapshot that restores this exact serializer on state recovery. */
        public static final class SerializerSnapshot
                extends SimpleTypeSerializerSnapshot<SortedListAccumulator> {
            public SerializerSnapshot() {
                super(SortedListAccumulatorSerializer::new);
            }
        }

        // ---- ClickStream field-level encode/decode -------------------------

        private static void serializeEvent(ClickStream cs, DataOutputView out)
                throws java.io.IOException {
            out.writeInt(cs.getUserId());
            out.writeUTF(cs.getSessionId());
            out.writeUTF(cs.getEventTime());
            out.writeUTF(cs.getEventName());
            out.writeLong(cs.getEventTimeMillis());
            Double price = cs.getPrice();
            if (price == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeDouble(price);
            }
        }

        private static ClickStream deserializeEvent(DataInputView in)
                throws java.io.IOException {
            int userId            = in.readInt();
            String sessionId      = in.readUTF();
            String eventTime      = in.readUTF();
            String eventName      = in.readUTF();
            long eventTimeMillis  = in.readLong();
            boolean hasPrice      = in.readBoolean();
            Double price          = hasPrice ? in.readDouble() : null;
            ClickStream cs = new ClickStream(userId, sessionId, eventTime, eventName, eventTimeMillis);
            cs.setPrice(price);
            return cs;
        }
    }

    /**
     * {@link TypeInformation} implementation that delegates serialization to
     * {@link SortedListAccumulatorSerializer}, preventing Flink from falling back to Kryo.
     */
    public static final class SortedListAccumulatorTypeInfo
            extends TypeInformation<SortedListAccumulator> {

        private static final long serialVersionUID = 1L;

        @Override public boolean isBasicType()  { return false; }
        @Override public boolean isTupleType()  { return false; }
        @Override public int     getArity()     { return 1; }
        @Override public int     getTotalFields(){ return 1; }
        @Override public Class<SortedListAccumulator> getTypeClass() {
            return SortedListAccumulator.class;
        }
        @Override public boolean isKeyType()    { return false; }

        @Override
        public TypeSerializer<SortedListAccumulator> createSerializer(SerializerConfig config) {
            return SortedListAccumulatorSerializer.INSTANCE;
        }

        @Override public String  toString()     { return "SortedListAccumulatorTypeInfo"; }
        @Override public boolean canEqual(Object obj) {
            return obj instanceof SortedListAccumulatorTypeInfo;
        }
        @Override public boolean equals(Object obj) {
            return obj instanceof SortedListAccumulatorTypeInfo;
        }
        @Override public int hashCode() { return SortedListAccumulatorTypeInfo.class.hashCode(); }
    }
}
