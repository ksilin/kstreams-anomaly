package com.example.kstreams.anomaly.model;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record FinTransaction(String sourceAccountId, String targetAccountId, long amount, String transactionId,
                             long timestamp) implements FinTransactionBuilder.With {
    public static FinTransaction empty() {
        return new FinTransaction("EMPTY", "EMPTY", 0, "EMPTY", 0);
    }
}
