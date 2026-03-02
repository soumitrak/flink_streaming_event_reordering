package com.example.clickstream.function;

import com.example.clickstream.model.ClickStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses a raw JSON string from the clickstream Kafka topic into a {@link ClickStream} object.
 *
 * <p>Records are dropped (not emitted) when any of the required fields
 * (user_id, session_id, event_time, event_name) is missing, null, or blank.
 * Any JSON parsing error also causes the record to be silently dropped and logged.
 */
public class ClickStreamParser implements FlatMapFunction<String, ClickStream> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);

    // ObjectMapper is not serializable, so mark transient and initialise lazily.
    private transient ObjectMapper objectMapper;

    @Override
    public void flatMap(String value, Collector<ClickStream> out) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        if (value == null || value.isBlank()) {
            LOG.warn("Received null or blank message – skipping.");
            return;
        }

        try {
            JsonNode root = objectMapper.readTree(value);

            // ---- extract required fields (top-level; nested fields are ignored) ----
            JsonNode userIdNode    = root.get("user_id");
            JsonNode sessionIdNode = root.get("session_id");
            JsonNode eventTimeNode = root.get("event_time");
            JsonNode eventNameNode = root.get("event_name");

            if (isNullOrMissing(userIdNode) || isNullOrMissing(sessionIdNode)
                    || isNullOrMissing(eventTimeNode) || isNullOrMissing(eventNameNode)) {
                LOG.warn("Dropping record with missing required field(s): {}", value);
                return;
            }

            int    userId    = userIdNode.asInt();
            String sessionId = sessionIdNode.asText().trim();
            String eventTime = eventTimeNode.asText().trim();
            String eventName = eventNameNode.asText().trim();

            if (sessionId.isEmpty() || eventTime.isEmpty() || eventName.isEmpty()) {
                LOG.warn("Dropping record with blank required field(s): {}", value);
                return;
            }

            // Validate / parse event time early so bad timestamps are caught here.
            long eventTimeMillis;
            try {
                eventTimeMillis = ClickStream.parseEventTimeMillis(eventTime);
            } catch (Exception e) {
                LOG.warn("Dropping record with unparseable event_time '{}': {}", eventTime, e.getMessage());
                return;
            }

            ClickStream cs = new ClickStream(userId, sessionId, eventTime, eventName);
            // eventTimeMillis is already derived inside the constructor, but we set it
            // explicitly to avoid a second parse.
            cs.setEventTimeMillis(eventTimeMillis);

            out.collect(cs);

        } catch (Exception e) {
            LOG.warn("Failed to parse clickstream record – skipping. Message: {}. Error: {}",
                    value, e.getMessage());
        }
    }

    /** Returns true when the node is absent, JSON-null, or an empty text value. */
    private boolean isNullOrMissing(JsonNode node) {
        return node == null || node.isNull() || node.isMissingNode();
    }
}
