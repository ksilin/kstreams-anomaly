package com.example.kstreams.anomaly.rules;

public class WindowedAmountAnomalyConfig {
    public final int thresholdAmount;
    public final long periodMs;

    public WindowedAmountAnomalyConfig(int thresholdAmount, long periodMs) {
        this.thresholdAmount = thresholdAmount;
        this.periodMs = periodMs;
    }
}
