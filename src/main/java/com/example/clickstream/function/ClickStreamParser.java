package com.example.clickstream.function;

import com.example.clickstream.model.ClickStream;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
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
 *
 * <p>Performance notes:
 * <ul>
 *   <li>Uses Jackson's streaming {@link JsonParser} instead of {@code readTree} to avoid
 *       allocating a full {@code JsonNode} tree. Only the five fields we care about are
 *       extracted.</li>
 *   <li>Parses the fixed-width event-time string by character position rather than
 *       via {@link java.time.format.DateTimeFormatter}, eliminating formatter overhead.</li>
 *   <li>Passes the pre-computed epoch millis directly to the {@link ClickStream} constructor
 *       so the timestamp is never parsed twice.</li>
 * </ul>
 */
public class ClickStreamParser implements FlatMapFunction<String, ClickStream> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);

    // JsonFactory is thread-safe; mark transient so Flink can serialize the operator.
    private transient JsonFactory jsonFactory;

    @Override
    public void flatMap(String value, Collector<ClickStream> out) {
        if (value == null || value.isEmpty()) {
            return;
        }

        if (jsonFactory == null) {
            jsonFactory = new JsonFactory();
        }

        int    userId      = 0;
        boolean userIdFound = false;
        String sessionId   = null;
        String eventTime   = null;
        String eventName   = null;
        double price       = Double.NaN;

        try (JsonParser p = jsonFactory.createParser(value)) {
            if (p.nextToken() != JsonToken.START_OBJECT) {
                LOG.warn("Dropping non-object record: {}", value);
                return;
            }

            String    field   = null;
            int       depth   = 1;   // 1 = inside root object
            boolean   inProps = false;
            JsonToken tok;

            while ((tok = p.nextToken()) != null) {
                switch (tok) {
                    case START_OBJECT:
                        depth++;
                        // depth==2 and the preceding field name is "properties"
                        inProps = (depth == 2 && "properties".equals(field));
                        break;
                    case END_OBJECT:
                        if (inProps && depth == 2) inProps = false;
                        depth--;
                        break;
                    case START_ARRAY:
                        depth++;
                        break;
                    case END_ARRAY:
                        depth--;
                        break;
                    case FIELD_NAME:
                        field = p.currentName();
                        break;
                    default:
                        if (field == null) break;
                        if (depth == 1) {
                            switch (field) {
                                case "user_id":
                                    userId = p.getIntValue();
                                    userIdFound = true;
                                    break;
                                case "session_id":
                                    sessionId = p.getText();
                                    break;
                                case "event_time":
                                    eventTime = p.getText();
                                    break;
                                case "event_name":
                                    eventName = p.getText();
                                    break;
                            }
                        } else if (inProps && "price".equals(field) && tok.isNumeric()) {
                            price = p.getDoubleValue();
                        }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse clickstream record – skipping. Message: {}. Error: {}",
                    value, e.getMessage());
            return;
        }

        if (!userIdFound
                || sessionId == null || sessionId.isEmpty()
                || eventTime == null || eventTime.isEmpty()
                || eventName == null || eventName.isEmpty()) {
            LOG.warn("Dropping record with missing required field(s): {}", value);
            return;
        }

        long eventTimeMillis;
        try {
            eventTimeMillis = ClickStream.parseEventTimeMillis(eventTime);
        } catch (Exception e) {
            LOG.warn("Dropping record with unparseable event_time '{}': {}", eventTime, e.getMessage());
            return;
        }

        ClickStream cs = new ClickStream(userId, sessionId, eventTime, eventName, eventTimeMillis);
        if (!Double.isNaN(price)) {
            cs.setPrice(price);
        }
        out.collect(cs);
    }
}
