package com.example.kstreams.fraud.model;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record TxAndWindowedAmount(FinTransaction tx, Long amount) implements TxAndWindowedAmountBuilder.With {

    public static TxAndWindowedAmount empty(){
        return new TxAndWindowedAmount(FinTransaction.empty(), 0L);
    }
}
