package com.example.clickstream.model;

import java.io.Serializable;
import java.time.LocalDate;

/**
 * Represents a single clickstream event extracted from the raw Kafka message.
 * Implements Comparable so events can be sorted by event time.
 */
public class ClickStream implements Serializable, Comparable<ClickStream> {

    private static final long serialVersionUID = 1L;

    // Format: dd/MM/yyyy HH:mm:ss.SSSSSS  (microseconds precision, exactly 26 chars)
    private static final int EVENT_TIME_LEN = 26;

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

    /** Fast path used by {@link com.example.clickstream.function.ClickStreamParser}: skips the
     *  timestamp parse when the caller has already computed {@code eventTimeMillis}. */
    public ClickStream(int userId, String sessionId, String eventTime, String eventName,
                       long eventTimeMillis) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.eventTime = eventTime;
        this.eventName = eventName;
        this.eventTimeMillis = eventTimeMillis;
    }

    /**
     * Parses "dd/MM/yyyy HH:mm:ss.SSSSSS" into epoch milliseconds (UTC) by extracting
     * each component directly from its fixed character position, avoiding
     * {@link java.time.format.DateTimeFormatter} overhead. Microseconds are truncated
     * to millisecond precision.
     */
    public static long parseEventTimeMillis(String eventTime) {
        String s = eventTime.trim();
        if (s.length() != EVENT_TIME_LEN) {
            throw new IllegalArgumentException("Unexpected event_time format: " + s);
        }
        int day    = twoDigit(s,  0);
        int month  = twoDigit(s,  3);
        int year   = fourDigit(s, 6);
        int hour   = twoDigit(s, 11);
        int minute = twoDigit(s, 14);
        int second = twoDigit(s, 17);
        int micros = sixDigit(s, 20);
        long epochDay = LocalDate.of(year, month, day).toEpochDay();
        return epochDay * 86_400_000L
                + hour   *  3_600_000L
                + minute *     60_000L
                + second *      1_000L
                + micros /      1_000L;
    }

    private static int twoDigit(String s, int i) {
        return (s.charAt(i) - '0') * 10 + (s.charAt(i + 1) - '0');
    }

    private static int fourDigit(String s, int i) {
        return (s.charAt(i)     - '0') * 1_000
                + (s.charAt(i + 1) - '0') * 100
                + (s.charAt(i + 2) - '0') * 10
                + (s.charAt(i + 3) - '0');
    }

    private static int sixDigit(String s, int i) {
        return (s.charAt(i)     - '0') * 100_000
                + (s.charAt(i + 1) - '0') *  10_000
                + (s.charAt(i + 2) - '0') *   1_000
                + (s.charAt(i + 3) - '0') *     100
                + (s.charAt(i + 4) - '0') *      10
                + (s.charAt(i + 5) - '0');
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
