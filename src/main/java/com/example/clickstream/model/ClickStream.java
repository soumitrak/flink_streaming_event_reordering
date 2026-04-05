package com.example.clickstream.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Represents a single clickstream event extracted from the raw Kafka message.
 * Implements Comparable so events can be sorted by event time.
 */
public class ClickStream implements Serializable, Comparable<ClickStream> {

    private static final long serialVersionUID = 1L;

    // Format: dd/MM/yyyy HH:mm:ss.SSSSSS  (microseconds precision)
    public static final DateTimeFormatter EVENT_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSSSSS");

    private int userId;
    private String sessionId;
    private String eventTime;       // original string, preserved for output
    private String eventName;
    private long eventTimeMillis;   // derived epoch-millis used for ordering and windowing
    private Double price;           // optional; populated from properties.price in the input JSON

    // Required for Flink POJO serialization
    public ClickStream() {}

    public ClickStream(int userId, String sessionId, String eventTime, String eventName) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.eventTime = eventTime;
        this.eventName = eventName;
        this.eventTimeMillis = parseEventTimeMillis(eventTime);
    }

    /**
     * Parses the event time string into epoch milliseconds (UTC).
     * The microsecond portion is truncated to milliseconds.
     */
    public static long parseEventTimeMillis(String eventTime) {
        LocalDateTime ldt = LocalDateTime.parse(eventTime.trim(), EVENT_TIME_FORMATTER);
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    @Override
    public int compareTo(ClickStream other) {
        return Long.compare(this.eventTimeMillis, other.eventTimeMillis);
    }

    // ------------------------------------------------------------------ getters/setters

    public int getUserId() { return userId; }
    public void setUserId(int userId) { this.userId = userId; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getEventTime() { return eventTime; }
    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
        this.eventTimeMillis = parseEventTimeMillis(eventTime);
    }

    public String getEventName() { return eventName; }
    public void setEventName(String eventName) { this.eventName = eventName; }

    public long getEventTimeMillis() { return eventTimeMillis; }
    public void setEventTimeMillis(long eventTimeMillis) { this.eventTimeMillis = eventTimeMillis; }

    public Double getPrice() { return price; }
    public void setPrice(Double price) { this.price = price; }

    @Override
    public String toString() {
        return "ClickStream{userId=" + userId + ", sessionId='" + sessionId + '\'' +
                ", eventTime='" + eventTime + '\'' + ", eventName='" + eventName + '\'' +
                ", price=" + price + '}';
    }
}
