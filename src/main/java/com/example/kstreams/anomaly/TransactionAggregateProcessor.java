package com.example.kstreams.anomaly;

import com.example.avro.Transaction;
import com.example.avro.TxAnomaly;
import com.example.avro.TxCheckResult;
import com.example.kstreams.anomaly.rules.*;
import io.vavr.control.Option;
import io.vavr.control.Validation;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class TransactionAggregateProcessor implements Processor<String, Transaction, String, TxCheckResult> {

    // TODO - distance between locations check
    // TODO - add suspicious locations check
    // TODO - add suspicious account names check
    private KeyValueStore<String, List<Transaction>> store;
    ProcessorContext<String, TxCheckResult> ctx;

    static final String TRANSACTION_AGGREGATE_STORE_NAME = "transactionAggregateStore";

    private MovementAnomalyConfig movementAnomalyCheck;

    public TransactionAggregateProcessor(long storeRetentionMs, SingleAmountAnomalyConfig singleAmountAnomalyConfig, WindowedAmountAnomalyConfig windowedAmountAnomalyConfig) {
        this.storeRetentionMs = storeRetentionMs;
        this.singleAmountAnomalyConfig = singleAmountAnomalyConfig;
        this.windowedAmountAnomalyConfig = windowedAmountAnomalyConfig;
    }

    private final long storeRetentionMs;

    private final SingleAmountAnomalyConfig singleAmountAnomalyConfig;
    private WindowedAmountAnomalyConfig windowedAmountAnomalyConfig;

    @Override
    public void init(ProcessorContext<String, TxCheckResult> context) {
        ctx = context;
        store = ctx.getStateStore(TRANSACTION_AGGREGATE_STORE_NAME);
    }

    @Override
    public void process(Record<String, Transaction> record) {

        // fetching and housekeeping - remove old transactions for the record key
        Option<List<Transaction>> maybeAggregation = Option.of(store.get(record.key())).map(txs -> txs.stream().filter(tx -> tx.getTimestamp() + storeRetentionMs > record.value().getTimestamp()).toList());

        Validation<TxAnomaly, Transaction> singleAmountAnomalyResult = SingleAmountAnomalyCheck.check(record.value(), singleAmountAnomalyConfig, ctx.currentSystemTimeMs());
        Validation<TxAnomaly, Transaction> windowedAmountAnomalyResult = WindowedAmountAnomalyCheck.check(maybeAggregation.getOrElse(List.of()), record.value(), windowedAmountAnomalyConfig, ctx.currentSystemTimeMs());

        Validation<List<TxAnomaly>, Transaction> aggregateValidations = aggregateValidations(io.vavr.collection.List.of(singleAmountAnomalyResult, windowedAmountAnomalyResult));

        TxCheckResult result = new TxCheckResult(record.value(), aggregateValidations.isInvalid() ? aggregateValidations.getError() : List.of());

        List<Transaction> transactions = new ArrayList<>(maybeAggregation.getOrElse(List.of()));
        transactions.add(record.value());
        store.put(record.key(), transactions);

        ctx.forward(record.withKey(record.key()).withValue(result));
    }

    private static Validation<List<TxAnomaly>, Transaction> aggregateValidations(io.vavr.collection.List<Validation<TxAnomaly, Transaction>> validations) {
        io.vavr.collection.List<TxAnomaly> errors = validations
                .filter(Validation::isInvalid)
                .flatMap(v -> List.of(v.getError()));

        return errors.isEmpty() ?
                Validation.valid(validations.head().get()) :
                Validation.invalid(errors.toJavaList());
    }
}
