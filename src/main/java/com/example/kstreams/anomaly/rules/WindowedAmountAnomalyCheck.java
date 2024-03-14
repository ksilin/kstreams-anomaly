package com.example.kstreams.anomaly.rules;

import com.example.avro.Transaction;
import com.example.avro.TxAnomaly;
import io.vavr.control.Validation;

import java.util.List;

public class WindowedAmountAnomalyCheck {

    public static final String WINDOWED_AGGREGATED_TX_AMOUNT_LIMIT_ANOMALY = "windowedAggregatedTxAmountOverLimit";
    public static Validation<TxAnomaly, Transaction> check(List<Transaction> previousTransactions, Transaction transaction, WindowedAmountAnomalyConfig config, long currentTimeMS) {

        long cutoffTime = currentTimeMS - config.periodMs;

        int previousSum = previousTransactions.stream().filter(t -> t.getTimestamp() > cutoffTime).map(Transaction::getTransactionAmount).reduce(0, Integer::sum);

        boolean isOverLimit = previousSum + transaction.getTransactionAmount() > config.thresholdAmount;
        if (isOverLimit) {
            String msg = String.format("Anomaly in transaction %s for account %s: aggregated amount %d over the last %d seconds is over limit of %d", transaction.getTransactionId(), transaction.getAccountName(), previousSum + transaction.getTransactionAmount(), Math.round(config.periodMs / 1000f), config.thresholdAmount);
            var anomaly = new TxAnomaly(transaction.getAccountName(), WINDOWED_AGGREGATED_TX_AMOUNT_LIMIT_ANOMALY, WindowedAmountAnomalyCheck.class.getSimpleName(), msg, transaction, currentTimeMS);
            return Validation.invalid(anomaly);
        } else {
            return Validation.valid(transaction);
        }
    }

}
