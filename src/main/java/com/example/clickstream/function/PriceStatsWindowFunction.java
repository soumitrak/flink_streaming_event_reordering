package com.example.clickstream.function;

import com.example.clickstream.model.CheckoutSession;
import com.example.clickstream.model.PriceStats;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.AggregatingMergeState;
import org.apache.flink.api.common.state.AggregatingMergeStateDescriptor;
import org.apache.flink.api.common.state.ReducingMergeState;
import org.apache.flink.api.common.state.ReducingMergeStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * Processing-time tumbling-window aggregation of checkout prices.
 *
 * <p>All {@link CheckoutSession} records are keyed by the constant {@code "global"}
 * (in {@link com.example.clickstream.ClickStreamJob}) so that min/max/average/count are
 * computed across every user and session within each window.
 *
 * <p>Sessions without a price ({@code price == null}) are silently ignored.
 *
 * <p><b>State layout</b> (per window, reset on every timer fire):
 * <ul>
 *   <li>{@code minPriceState} – {@link ReducingMergeState}{@code <Double>} backed by
 *       {@link org.apache.flink.state.rocksdb.RocksDBReducingMergeState}. Each {@code add(v)}
 *       issues a {@code db.merge()} call (write-only, no read). The {@code Math::min} function
 *       is applied lazily by the RocksDB merge operator during compaction or on {@code get()}.</li>
 *   <li>{@code maxPriceState} – {@link ReducingMergeState}{@code <Double>} with {@code Math::max};
 *       same RocksDB write-only merge path as {@code minPriceState}.</li>
 *   <li>{@code avgPriceState} – {@link AggregatingMergeState}{@code <Double, AvgAccumulator, Double>}
 *       backed by {@link org.apache.flink.state.rocksdb.RocksDBAggregatingMergeState}. Input
 *       {@code Double} values are serialised as merge operands; the {@link AvgAggregateFunction}
 *       folds operands into the accumulator lazily on {@code get()}.</li>
 *   <li>{@code countState} – standard {@link ReducingState}{@code <Long>} registered via
 *       {@link ReducingStateDescriptor} through the normal {@link
 *       org.apache.flink.api.common.functions.RuntimeContext} API.</li>
 *   <li>{@code windowEndState} – {@link ValueState}{@code <Long>} tracking the epoch-ms at which
 *       the current window timer fires (read/written once per window).</li>
 * </ul>
 *
 * <p><b>Why reflection for merge states:</b> {@link ReducingMergeStateDescriptor} and
 * {@link AggregatingMergeStateDescriptor} must be registered through
 * {@link KeyedStateBackend#getPartitionedState}, which is not exposed on the standard
 * {@link org.apache.flink.api.common.functions.RuntimeContext} API. The backend is reached by
 * reflectively reading the private {@code keyedStateStore} field of
 * {@link StreamingRuntimeContext} and the protected {@code keyedStateBackend} field of
 * {@link DefaultKeyedStateStore}.
 */
public class PriceStatsWindowFunction
        extends KeyedProcessFunction<String, CheckoutSession, PriceStats> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PriceStatsWindowFunction.class);

    /** Width of the processing-time tumbling window. */
    private static final long WINDOW_SIZE_MS = 5L * 60 * 1_000;

    // ------------------------------------------------------------------ state

    /**
     * Minimum price in the window.
     * Backed by {@code RocksDBReducingMergeState}: each {@code add()} is a pure
     * {@code db.merge()} append with no read on the hot write path.
     */
    private ReducingMergeState<Double> minPriceState;

    /**
     * Maximum price in the window.
     * Same {@code RocksDBReducingMergeState} optimisation as {@link #minPriceState}.
     */
    private ReducingMergeState<Double> maxPriceState;

    /**
     * Running (sum, count) accumulator for the average price.
     * Backed by {@code RocksDBAggregatingMergeState}: input {@code Double} values are
     * appended as raw merge operands and folded lazily by {@link AvgAggregateFunction}.
     */
    private AggregatingMergeState<Double, Double> avgPriceState;

    /**
     * Number of priced checkout events in the window.
     * Uses the standard {@link ReducingState} (read-modify-write) registered through
     * the normal {@link org.apache.flink.api.common.functions.RuntimeContext} API.
     */
    private ReducingState<Long> countState;

    /**
     * Epoch-ms at which the current window's processing-time timer will fire.
     * Cleared after each timer to force a new registration on the next event.
     */
    private ValueState<Long> windowEndState;

    // ------------------------------------------------------------------ lifecycle

    @Override
    public void open(OpenContext parameters) throws Exception {
        // Obtain the KeyedStateBackend via reflection so we can register merge-operator-backed
        // state types, which are not exposed through the standard RuntimeContext API.
        @SuppressWarnings("unchecked")
        KeyedStateBackend<String> keyedStateBackend = extractKeyedStateBackend();

        minPriceState = getRuntimeContext().getReducingMergeState(new ReducingMergeStateDescriptor<>("min-price", Math::min, DoubleSerializer.INSTANCE));
        maxPriceState = getRuntimeContext().getReducingMergeState(new ReducingMergeStateDescriptor<>("max-price", Math::max, DoubleSerializer.INSTANCE));
        avgPriceState = getRuntimeContext().getAggregatingMergeState(new AggregatingMergeStateDescriptor<>(
                "avg-price",
                new AvgAggregateFunction(),
                DoubleSerializer.INSTANCE,
                TypeInformation.of(new TypeHint<AvgAccumulator>() {})));
/*
        // ReducingMergeState for minimum – backed by RocksDBReducingMergeState
        minPriceState = keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ReducingMergeStateDescriptor<>("min-price", Math::min, DoubleSerializer.INSTANCE));

        // ReducingMergeState for maximum – backed by RocksDBReducingMergeState
        maxPriceState = keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ReducingMergeStateDescriptor<>("max-price", Math::max, DoubleSerializer.INSTANCE));

        // AggregatingMergeState for average – backed by RocksDBAggregatingMergeState.
        // DoubleSerializer is the input (merge-operand) serializer; the accumulator type
        // AvgAccumulator is resolved via TypeInformation for Flink POJO serialization.
        avgPriceState = keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new AggregatingMergeStateDescriptor<>(
                        "avg-price",
                        new AvgAggregateFunction(),
                        DoubleSerializer.INSTANCE,
                        TypeInformation.of(new TypeHint<AvgAccumulator>() {})));
*/

        // Standard ReducingState for count – registered through the normal RuntimeContext API
        countState = getRuntimeContext().getReducingState(
                new ReducingStateDescriptor<>("count", Long::sum, Long.class));

        // Standard ValueState for the window boundary timestamp (read/written once per window)
        windowEndState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("window-end", Long.class));
    }

    // ------------------------------------------------------------------ processElement

    @Override
    public void processElement(CheckoutSession session,
                               Context ctx,
                               Collector<PriceStats> out) throws Exception {
        Double price = session.getPrice();
        if (price == null) {
            return; // price is optional; skip sessions that have none
        }

        // Merge-state adds: each issues a db.merge() — write-only, no read of existing state.
        minPriceState.add(price);
        maxPriceState.add(price);
        avgPriceState.add(price);
        // Standard ReducingState add: read-modify-write through RuntimeContext
        countState.add(1L);

        // Register a processing-time timer at the next window boundary if not yet registered.
        Long windowEnd = windowEndState.value();
        if (windowEnd == null) {
            long now = ctx.timerService().currentProcessingTime();
            // Round up to the next WINDOW_SIZE_MS boundary.
            windowEnd = ((now / WINDOW_SIZE_MS) + 1) * WINDOW_SIZE_MS;
            ctx.timerService().registerProcessingTimeTimer(windowEnd);
            windowEndState.update(windowEnd);
            LOG.debug("Registered window timer at {} (now={})", windowEnd, now);
        }
    }

    // ------------------------------------------------------------------ onTimer

    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<PriceStats> out) throws Exception {
        // get() folds all accumulated merge operands lazily in a single RocksDB read.
        Double minPrice = minPriceState.get();
        Double maxPrice = maxPriceState.get();
        Double avgPrice = avgPriceState.get();
        Long   count    = countState.get();

        if (minPrice != null && maxPrice != null && avgPrice != null && count != null) {
            long windowStart = timestamp - WINDOW_SIZE_MS;
            PriceStats stats = new PriceStats(windowStart, timestamp, minPrice, maxPrice, avgPrice, count);
            LOG.info("Emitting price stats for window [{}, {}): min={}, max={}, avg={}, count={}",
                    windowStart, timestamp, minPrice, maxPrice, avgPrice, count);
            out.collect(stats);
        } else {
            LOG.debug("Window [{}, {}) had no priced checkouts – nothing emitted.",
                    timestamp - WINDOW_SIZE_MS, timestamp);
        }

        // Clear all state so the next window starts fresh.
        minPriceState.clear();
        maxPriceState.clear();
        avgPriceState.clear();
        countState.clear();
        windowEndState.clear();
    }

    // ------------------------------------------------------------------ reflection helper

    /**
     * Reaches through two layers of encapsulation to obtain the live {@link KeyedStateBackend}
     * that backs this operator's keyed state.
     *
     * <p>{@link ReducingMergeStateDescriptor} and {@link AggregatingMergeStateDescriptor} must be
     * registered via {@link KeyedStateBackend#getPartitionedState}, which is not part of the
     * public {@link org.apache.flink.api.common.functions.RuntimeContext} API. The path is:
     * <pre>
     *   StreamingRuntimeContext  (private field: keyedStateStore)
     *     └─ DefaultKeyedStateStore  (protected field: keyedStateBackend)
     *          └─ KeyedStateBackend  ← we need this
     * </pre>
     */
    @SuppressWarnings("unchecked")
    private KeyedStateBackend<String> extractKeyedStateBackend() {
        try {
            // Step 1: get the private keyedStateStore field from StreamingRuntimeContext
            Field storeField = StreamingRuntimeContext.class.getDeclaredField("keyedStateStore");
            storeField.setAccessible(true);
            DefaultKeyedStateStore store =
                    (DefaultKeyedStateStore) storeField.get(getRuntimeContext());

            // Step 2: get the protected keyedStateBackend field from DefaultKeyedStateStore
            Field backendField =
                    DefaultKeyedStateStore.class.getDeclaredField("keyedStateBackend");
            backendField.setAccessible(true);
            return (KeyedStateBackend<String>) backendField.get(store);

        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Cannot access KeyedStateBackend from RuntimeContext. "
                    + "Ensure PriceStatsWindowFunction runs with the RocksDB state backend "
                    + "and within a keyed stream context.", e);
        }
    }

    // ------------------------------------------------------------------ AvgAccumulator

    /**
     * Accumulator for the running average calculation.
     * Must be a public static POJO so Flink's type system can serialise it as the
     * accumulator stored in {@link AggregatingMergeState} /
     * {@link org.apache.flink.state.rocksdb.RocksDBAggregatingMergeState}.
     */
    public static class AvgAccumulator implements Serializable {

        private static final long serialVersionUID = 1L;

        public double sum;
        public long   count;

        public AvgAccumulator() {}

        public AvgAccumulator(double sum, long count) {
            this.sum   = sum;
            this.count = count;
        }
    }

    // ------------------------------------------------------------------ AvgAggregateFunction

    /**
     * {@link AggregateFunction} that maintains a running {@link AvgAccumulator} and
     * computes the average on {@link #getResult}.
     *
     * <p>The {@link #merge} method is called by
     * {@link org.apache.flink.state.rocksdb.RocksDBAggregatingMergeState} during RocksDB
     * compaction and on {@code get()} to fold partial accumulators from different merge operands.
     */
    public static class AvgAggregateFunction
            implements AggregateFunction<Double, AvgAccumulator, Double> {

        private static final long serialVersionUID = 1L;

        @Override
        public AvgAccumulator createAccumulator() {
            return new AvgAccumulator(0.0, 0L);
        }

        @Override
        public AvgAccumulator add(Double price, AvgAccumulator acc) {
            acc.sum   += price;
            acc.count += 1;
            return acc;
        }

        @Override
        public Double getResult(AvgAccumulator acc) {
            double result = acc.count > 0 ? acc.sum / acc.count : 0.0;
            return result;
        }

        @Override
        public AvgAccumulator merge(AvgAccumulator a, AvgAccumulator b) {
            AvgAccumulator merged = new AvgAccumulator(a.sum + b.sum, a.count + b.count);
            return merged;
        }
    }
}
