package com.example.kstreams.fraud.model;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record TxsAndWindowedAmount(List<FinTransaction> txs, Long amount) implements TxsAndWindowedAmountBuilder.With {

    public static TxsAndWindowedAmount empty(){
        return new TxsAndWindowedAmount(List.of(), 0L);
    }
}
