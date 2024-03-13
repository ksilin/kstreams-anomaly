package com.example.kstreams.anomaly.testdata;

import com.example.avro.Transaction;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TxGenerator {
    private final List<AccountConfig> accountConfigs;
    private final Random random = ThreadLocalRandom.current();

    public TxGenerator() {
        accountConfigs = List.of(
                new AccountConfig("Edward Teach", 5, new Double[]{35.0, -75.0}, 0.1, 10),
                new AccountConfig("Maxwell Edison", 10, new Double[]{51.5, -0.1}, 0.1, 20),
                new AccountConfig("Anne Bonny", 50, new Double[]{32.5, -10.1}, 0.1, 80),
                new AccountConfig("Mary Read", 10, new Double[]{3.5, -100.1}, 0.1, 10),
                new AccountConfig("Stede Bonnet", 10, new Double[]{21.5, 40.1}, 0.1, 30),
                // Define other accounts with their own configurations...
                new AccountConfig("Ben Spender", 3, new Double[]{52.5, 13.4}, 0.1, 300),
                new AccountConfig("Samuel Bellamy", 5, new Double[]{0.0, 0.0}, 10.0, 30)
        );
    }

    public Transaction createTransaction() {
        AccountConfig selectedConfig = selectAccountConfig();
        Integer transactionAmount = random.nextInt(selectedConfig.maxAmount) + 1;
        String transactionId = UUID.randomUUID().toString();
        Double lat = selectedConfig.baseCoordinates[0] + (random.nextDouble() - 0.5) * selectedConfig.variability;
        Double lon = selectedConfig.baseCoordinates[1] + (random.nextDouble() - 0.5) * selectedConfig.variability;

        return new Transaction(selectedConfig.name, transactionAmount, transactionId, lat, lon, System.currentTimeMillis());
    }

    private AccountConfig selectAccountConfig() {
        double totalProbability = accountConfigs.stream().mapToDouble(a -> a.probability).sum();
        double value = random.nextDouble() * totalProbability;
        double sum = 0;
        for (AccountConfig config : accountConfigs) {
            sum += config.probability;
            if (value < sum) {
                return config;
            }
        }
        throw new IllegalStateException("Should not reach here if probabilities are configured correctly");
    }

    static class AccountConfig {
        String name;
        double probability;
        Double[] baseCoordinates;
        double variability;
        int maxAmount;

        public AccountConfig(String name, double probability, Double[] baseCoordinates, double variability, int maxAmount) {
            this.name = name;
            this.probability = probability;
            this.baseCoordinates = baseCoordinates;
            this.variability = variability;
            this.maxAmount = maxAmount;
        }
    }

    // Assume Transaction class is defined elsewhere...
}
