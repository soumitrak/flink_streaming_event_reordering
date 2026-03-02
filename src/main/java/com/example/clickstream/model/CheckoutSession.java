package com.example.clickstream.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Represents the output record emitted when a Checkout sequence is identified.
 */
public class CheckoutSession implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("user_id")
    private int userId;

    @JsonProperty("session_id")
    private String sessionId;

    /** event_time of the first event in the tracked sequence */
    @JsonProperty("start_time")
    private String startTime;

    /** event_time of the Checkout event */
    @JsonProperty("end_time")
    private String endTime;

    /** end_time - start_time in seconds */
    @JsonProperty("duration")
    private long duration;

    /** Ordered list of event_names in the tracked sequence leading to Checkout */
    @JsonProperty("event_names")
    private List<String> eventNames;

    // Required for Flink / Jackson serialization
    public CheckoutSession() {}

    public CheckoutSession(int userId, String sessionId, String startTime, String endTime,
                           long duration, List<String> eventNames) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
        this.eventNames = eventNames;
    }

    // ------------------------------------------------------------------ getters/setters

    public int getUserId() { return userId; }
    public void setUserId(int userId) { this.userId = userId; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getStartTime() { return startTime; }
    public void setStartTime(String startTime) { this.startTime = startTime; }

    public String getEndTime() { return endTime; }
    public void setEndTime(String endTime) { this.endTime = endTime; }

    public long getDuration() { return duration; }
    public void setDuration(long duration) { this.duration = duration; }

    public List<String> getEventNames() { return eventNames; }
    public void setEventNames(List<String> eventNames) { this.eventNames = eventNames; }

    @Override
    public String toString() {
        return "CheckoutSession{userId=" + userId + ", sessionId='" + sessionId + '\'' +
                ", startTime='" + startTime + '\'' + ", endTime='" + endTime + '\'' +
                ", duration=" + duration + ", eventNames=" + eventNames + '}';
    }
}
