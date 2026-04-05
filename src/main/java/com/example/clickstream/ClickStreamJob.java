package com.example.clickstream;

import com.example.clickstream.function.ClickStreamParser;
import com.example.clickstream.function.ClickStreamProcessFunction;
import com.example.clickstream.function.PriceStatsWindowFunction;
import com.example.clickstream.model.CheckoutSession;
import com.example.clickstream.model.ClickStream;
import com.example.clickstream.model.PriceStats;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Entry point for the Flink Clickstream Event Reordering job.
 *
 * <p>Pipeline:
 * <pre>
 *  Kafka (clickstream)
 *      → parse &amp; filter (ClickStreamParser)
 *      → keyBy (user_id + "_" + session_id)
 *      → KeyedProcessFunction (ClickStreamProcessFunction)  ← reorder + detect checkout
 *      → serialize to JSON string
 *      → Kafka (checkout-session)
 * </pre>
 *
 * <p>Configuration is read from the following environment variables (with defaults
 * suitable for local development):
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS}    – default {@code localhost:9092}</li>
 *   <li>{@code KAFKA_INPUT_TOPIC}          – default {@code clickstream}</li>
 *   <li>{@code KAFKA_OUTPUT_TOPIC}         – default {@code checkout-session}</li>
 *   <li>{@code KAFKA_PRICE_STATS_TOPIC}    – default {@code price-stats}</li>
 *   <li>{@code KAFKA_CONSUMER_GROUP}       – default {@code clickstream-reordering}</li>
 * </ul>
 */
public class ClickStreamJob {

    public static void main(String[] args) throws Exception {

        // ------------------------------------------------------------------ configuration
        String bootstrapServers  = envOrDefault("KAFKA_BOOTSTRAP_SERVERS",  "localhost:9092");
        String inputTopic        = envOrDefault("KAFKA_INPUT_TOPIC",         "clickstream");
        String outputTopic       = envOrDefault("KAFKA_OUTPUT_TOPIC",        "checkout-session");
        String priceStatsTopic   = envOrDefault("KAFKA_PRICE_STATS_TOPIC",   "price-stats");
        String consumerGroup     = envOrDefault("KAFKA_CONSUMER_GROUP",      "clickstream-reordering");

        // ------------------------------------------------------------------ Flink env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Switch to RocksDB state backend so the job uses disk-spilling incremental state
        // (EmbeddedRocksDBStateBackend) instead of the default in-memory HashMap backend.
        // The factory class is resolved from the fat JAR at runtime via the "rocksdb" alias.
        Configuration rocksdbConfig = new Configuration();
        rocksdbConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        env.configure(rocksdbConfig);

        // ------------------------------------------------------------------ Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(consumerGroup)
                // Start from the earliest uncommitted offset on first run;
                // resume from committed offset on restart.
                .setStartingOffsets(
                        OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // We use WatermarkStrategy.noWatermarks() because the KeyedProcessFunction
        // performs its own event-time tracking with processing-time timers.
        DataStream<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-clickstream-source");

        // ------------------------------------------------------------------ pipeline
        DataStream<CheckoutSession> checkoutStream = rawStream
                // Parse JSON → ClickStream (invalid records are silently dropped)
                .flatMap(new ClickStreamParser())
                .name("parse-and-filter")

                // Key by (user_id, session_id) so each user's session is processed together.
                .keyBy(cs -> cs.getUserId() + "_" + cs.getSessionId())

                // Reorder events and detect checkout sequences.
                .process(new ClickStreamProcessFunction())
                .name("reorder-and-detect-checkout");

        // ------------------------------------------------------------------ Kafka sink
        Properties producerProps = new Properties();
        // Idempotent producer for exactly-once semantics (when combined with checkpointing).
        producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        checkoutStream
                .map(new CheckoutSessionSerializer())
                .name("serialize-to-json")
                .sinkTo(kafkaSink)
                .name("Kafka-checkout-session-sink");

        // ------------------------------------------------------------------ price-stats sink
        KafkaSink<String> priceStatsSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(priceStatsTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        // 5-minute processing-time tumbling window: aggregate min, max, and average checkout
        // prices using ReducingState (→ RocksDBReducingMergeState) for min/max and
        // AggregatingState (→ RocksDBAggregatingMergeState) for the running average.
        // Key by constant "global" so all sessions contribute to the same aggregation.
        checkoutStream
                .keyBy(session -> "global")
                .process(new PriceStatsWindowFunction())
                .name("price-stats-5min-window")
                .map(new PriceStatsSerializer())
                .name("serialize-price-stats")
                .sinkTo(priceStatsSink)
                .name("Kafka-price-stats-sink");

        // ------------------------------------------------------------------ execute
        env.execute("Flink Clickstream Event Reordering");
    }

    // ------------------------------------------------------------------ helpers

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }

    /**
     * Serializes a {@link CheckoutSession} to a compact JSON string.
     * The ObjectMapper is created lazily (not serializable, so cannot be a field).
     */
    private static class CheckoutSessionSerializer implements MapFunction<CheckoutSession, String> {

        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public String map(CheckoutSession session) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
                mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
            }
            return mapper.writeValueAsString(session);
        }
    }

    /**
     * Serializes a {@link PriceStats} to a compact JSON string.
     */
    private static class PriceStatsSerializer implements MapFunction<PriceStats, String> {

        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public String map(PriceStats stats) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
                mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
            }
            return mapper.writeValueAsString(stats);
        }
    }
}
