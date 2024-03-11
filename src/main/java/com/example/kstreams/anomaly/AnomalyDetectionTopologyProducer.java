package com.example.kstreams.anomaly;

import com.example.avro.Transaction;
import com.example.avro.TxAnomaly;
import com.example.avro.TxCheckResult;
import com.example.kstreams.anomaly.rules.SingleAmountAnomalyConfig;
import com.example.kstreams.anomaly.rules.WindowedAmountAnomalyConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class AnomalyDetectionTopologyProducer {

    static int maxAmount = 100;
    static long windowDurationMs = 10000;
    static long graceDurationMs = 1000;

    Logger log = Logger.getLogger(AnomalyDetectionTopologyProducer.class);

    String sourceTopic;
    String validTopic;
    String invalidTopic;
    String srUrl;

    static final Serde<Transaction> txSerde = new SpecificAvroSerde<>();
    static final Serde<TxAnomaly> txAnomalySerde = new SpecificAvroSerde<>();
    static final Serde<TxCheckResult> txCheckResultSerde = new SpecificAvroSerde<>();

    @Inject
    public AnomalyDetectionTopologyProducer(@ConfigProperty(name = "anomaly.sourceTopic") String sourceTopic,
                                            @ConfigProperty(name = "anomaly.validTopic") String validTopic,
                                            @ConfigProperty(name = "anomaly.invalidTopic") String invalidTopic,
                                            @ConfigProperty(name = "schema.registry.url") String srUrl) {
        this.sourceTopic = sourceTopic;
        this.validTopic = validTopic;
        this.invalidTopic = invalidTopic;
        this.srUrl = srUrl;

    }

    @Produces
    public Topology produceTopology() {
        long storeRetentionMs = 10000;
        SingleAmountAnomalyConfig singleAmountAnomalyConfig = new SingleAmountAnomalyConfig(100);
        WindowedAmountAnomalyConfig windowedAmountAnomalyConfig = new WindowedAmountAnomalyConfig(200, 1000);
        ProcessorSupplier<String, Transaction, String, TxCheckResult> txCheckResultProcessorSupplier = () -> new TransactionAggregateProcessor(storeRetentionMs, singleAmountAnomalyConfig, windowedAmountAnomalyConfig);
        return createTopology(this.sourceTopic, this.validTopic, this.invalidTopic,  this.srUrl, txCheckResultProcessorSupplier);
    }


    public static Topology createTopology(String sourceTopic, String validTopic, String invalidTopic, String srUrl, ProcessorSupplier<String, Transaction, String, TxCheckResult> txCheckResultProcessorSupplier) {

        configureSerdes(srUrl);

        var builder = new StreamsBuilder();

        Serdes.ListSerde<TxCheckResult> checkResultListSerde = new Serdes.ListSerde<>(ArrayList.class, txCheckResultSerde);
        Serdes.ListSerde<TxAnomaly> anomalyListSerde = new Serdes.ListSerde<>(ArrayList.class, txAnomalySerde);

        var storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(TransactionAggregateProcessor.TRANSACTION_AGGREGATE_STORE_NAME), Serdes.String(), checkResultListSerde);
        builder.addStateStore(storeBuilder);

        var stream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), txSerde));
        //stream.foreach((k, v) -> log.infof("k: %s, v: %s", k, v));

        KStream<String, TxCheckResult> checkResultsStream = stream.process(txCheckResultProcessorSupplier, TransactionAggregateProcessor.TRANSACTION_AGGREGATE_STORE_NAME);

        String checkResultsSplit = "checkResultsSplit-";
        String validBranch = "validBranch";
        String anomaliesBranch = "anomaliesBranch";

        BranchedKStream<String, TxCheckResult> checkResultBranchedKStream = checkResultsStream.split(Named.as(checkResultsSplit));
        checkResultBranchedKStream.branch((k, v) -> !v.getAnomalies().isEmpty(), Branched.as(anomaliesBranch));
        Map<String, KStream<String, TxCheckResult>> branchMap = checkResultBranchedKStream.defaultBranch(Branched.as(validBranch));

        KStream<String, TxCheckResult> anomaliesStream = branchMap.get(checkResultsSplit + anomaliesBranch);
        KStream<String, TxCheckResult> validTxCheckStream = branchMap.get(checkResultsSplit + validBranch);

        KStream<String, List<TxAnomaly>> mappedValues = anomaliesStream.mapValues(TxCheckResult::getAnomalies);
        mappedValues.to(invalidTopic, Produced.with(Serdes.String(), anomalyListSerde));
        KStream<String, Transaction> validTxStream = validTxCheckStream.mapValues(TxCheckResult::getTransaction);
        validTxStream.to(validTopic, Produced.with(Serdes.String(), txSerde));

        return builder.build();
    }

    private static void configureSerdes(String srUrl) {
        Map<String, String> serdeConfig = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl,
                                                 AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, "true"
        );
        txSerde.configure(serdeConfig, false);
        txAnomalySerde.configure(serdeConfig, false);
        txCheckResultSerde.configure(serdeConfig, false);
    }
}