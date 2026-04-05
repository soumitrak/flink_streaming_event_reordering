package com.example.clickstream.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Output record emitted at the end of each 5-minute tumbling window by
 * {@link com.example.clickstream.function.PriceStatsWindowFunction}.
 *
 * <p>Contains the window boundaries (epoch-ms) and the aggregated
 * min/max/average checkout prices and event count observed within the window.
 */
public class PriceStats implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Epoch-ms of the inclusive window start. */
    @JsonProperty("window_start")
    private long windowStart;

    /** Epoch-ms of the exclusive window end (= the timer timestamp). */
    @JsonProperty("window_end")
    private long windowEnd;

    @JsonProperty("min_price")
    private double minPrice;

    @JsonProperty("max_price")
    private double maxPrice;

    @JsonProperty("avg_price")
    private double avgPrice;

    /** Number of priced checkout events that contributed to this window's statistics. */
    @JsonProperty("count")
    private long count;

    // Required for Flink / Jackson serialization
    public PriceStats() {}

    public PriceStats(long windowStart, long windowEnd,
                      double minPrice, double maxPrice, double avgPrice, long count) {
        this.windowStart = windowStart;
        this.windowEnd   = windowEnd;
        this.minPrice    = minPrice;
        this.maxPrice    = maxPrice;
        this.avgPrice    = avgPrice;
        this.count       = count;
    }

    // ------------------------------------------------------------------ getters/setters

    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    public double getMinPrice() { return minPrice; }
    public void setMinPrice(double minPrice) { this.minPrice = minPrice; }

    public double getMaxPrice() { return maxPrice; }
    public void setMaxPrice(double maxPrice) { this.maxPrice = maxPrice; }

    public double getAvgPrice() { return avgPrice; }
    public void setAvgPrice(double avgPrice) { this.avgPrice = avgPrice; }

    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }

    @Override
    public String toString() {
        return "PriceStats{windowStart=" + windowStart + ", windowEnd=" + windowEnd +
                ", minPrice=" + minPrice + ", maxPrice=" + maxPrice +
                ", avgPrice=" + avgPrice + ", count=" + count + '}';
    }
}
