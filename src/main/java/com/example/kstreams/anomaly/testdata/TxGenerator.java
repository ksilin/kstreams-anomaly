package com.example.kstreams.anomaly.testdata;

import com.example.avro.Transaction;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TxGenerator {
    private final List<TxGeneratorAccountConfig> accountConfigs;
    private final Random random = ThreadLocalRandom.current();

    public TxGenerator() {
        accountConfigs = List.of(
                new TxGeneratorAccountConfig("Edward Teach", 5, new Double[]{35.0, -75.0}, 0.1, 10),
                new TxGeneratorAccountConfig("Maxwell Edison", 10, new Double[]{51.5, -0.1}, 0.1, 20),
                new TxGeneratorAccountConfig("Anne Bonny", 50, new Double[]{32.5, -10.1}, 0.1, 80),
                new TxGeneratorAccountConfig("Mary Read", 10, new Double[]{3.5, -100.1}, 0.1, 10),
                new TxGeneratorAccountConfig("Stede Bonnet", 10, new Double[]{21.5, 40.1}, 0.1, 30),
                new TxGeneratorAccountConfig("Ben Spender", 3, new Double[]{52.5, 13.4}, 0.1, 300),
                new TxGeneratorAccountConfig("Samuel Bellamy", 5, new Double[]{0.0, 0.0}, 10.0, 30)
        );
    }

    public Transaction createTransaction() {
        TxGeneratorAccountConfig selectedConfig = selectAccountConfig();
        Integer transactionAmount = random.nextInt(selectedConfig.maxAmount) + 1;
        String transactionId = UUID.randomUUID().toString();
        Double lat = selectedConfig.baseCoordinates[0] + (random.nextDouble() - 0.5) * selectedConfig.variability;
        Double lon = selectedConfig.baseCoordinates[1] + (random.nextDouble() - 0.5) * selectedConfig.variability;

        return new Transaction(selectedConfig.name, transactionAmount, transactionId, lat, lon, System.currentTimeMillis());
    }

    private TxGeneratorAccountConfig selectAccountConfig() {
        double totalProbability = accountConfigs.stream().mapToDouble(a -> a.probability).sum();
        double value = random.nextDouble() * totalProbability;
        double sum = 0;
        for (TxGeneratorAccountConfig config : accountConfigs) {
            sum += config.probability;
            if (value < sum) {
                return config;
            }
        }
        throw new IllegalStateException("Should not reach here if probabilities are configured correctly");
    }

    public record TxGeneratorAccountConfig(String name, double probability, Double[] baseCoordinates, double variability, int maxAmount) {
    }
}
