package com.example.kstreams.anomaly.rules;

import com.example.avro.Transaction;
import com.example.avro.TxAnomaly;
import io.vavr.control.Validation;

public class SingleAmountAnomalyCheck {

    public static final String SINGLE_TX_AMOUNT_LIMIT_ANOMALY = "singleTxAmountOverLimit";

    public static Validation<TxAnomaly, Transaction> check(Transaction transaction, SingleAmountAnomalyConfig config, long currentTimeMS) {

        boolean isOverLimit = transaction.getTransactionAmount() > config.limit();
        if (isOverLimit) {
            String msg = String.format("Anomaly in transaction %s for account %s: amount %d is over single transaction limit of %d", transaction.getTransactionId(), transaction.getAccountName(), transaction.getTransactionAmount(), config.limit());
            var anomaly = new TxAnomaly(transaction.getAccountName(), SINGLE_TX_AMOUNT_LIMIT_ANOMALY, SingleAmountAnomalyCheck.class.getSimpleName(), msg, transaction, currentTimeMS);
            return Validation.invalid(anomaly);
        } else {
            return Validation.valid(transaction);
        }
    }

}
