package com.example.kstreams.anomaly;

import com.example.avro.Transaction;
import com.example.avro.TxAnomaly;
import com.example.avro.TxCheckResult;
import com.example.kstreams.anomaly.rules.SingleAmountAnomalyConfig;
import com.example.kstreams.anomaly.rules.WindowedAmountAnomalyConfig;
import com.example.kstreams.anomaly.util.AvroSerdes;
import com.example.kstreams.anomaly.util.SrConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Map;

@ApplicationScoped
public class AnomalyDetectionTopologyProducer {

    Logger log = Logger.getLogger(AnomalyDetectionTopologyProducer.class);

    String sourceTopic;
    String validTopic;
    String invalidTopic;
    SrConfig srConfig;

    static final String checkResultsSplit = "checkResultsSplit-";
    static final String validBranch = "validBranch";
    static final String anomaliesBranch = "anomaliesBranch";

    @Inject
    public AnomalyDetectionTopologyProducer(@ConfigProperty(name = "anomaly.sourceTopic") String sourceTopic,
                                            @ConfigProperty(name = "anomaly.validTopic") String validTopic,
                                            @ConfigProperty(name = "anomaly.invalidTopic") String invalidTopic,
                                            SrConfig srConfig
    ) {
        this.sourceTopic = sourceTopic;
        this.validTopic = validTopic;
        this.invalidTopic = invalidTopic;

        this.srConfig = srConfig;
    }

    @Produces
    public Topology produceTopology() {
        log.infov("producing topology with SR properties {0}, {1}", srConfig.srUrl, srConfig.basicAuthUserInfo);
        long storeRetentionMs = 100000;
        SingleAmountAnomalyConfig singleAmountAnomalyConfig = new SingleAmountAnomalyConfig(100);
        WindowedAmountAnomalyConfig windowedAmountAnomalyConfig = new WindowedAmountAnomalyConfig(300, 10000);
        ProcessorSupplier<String, Transaction, String, TxCheckResult> txCheckResultProcessorSupplier = () -> new TransactionAggregateProcessor(storeRetentionMs, singleAmountAnomalyConfig, windowedAmountAnomalyConfig);
        return createTopology(this.sourceTopic, this.validTopic, this.invalidTopic,  this.srConfig, txCheckResultProcessorSupplier);
    }


    public static Topology createTopology(String sourceTopic, String validTopic, String invalidTopic, SrConfig srConfig, ProcessorSupplier<String, Transaction, String, TxCheckResult> txCheckResultProcessorSupplier) {

        //configureSerdes(srUrl, basicAuthUserInfo);

        var builder = new StreamsBuilder();

        Serdes.ListSerde<TxCheckResult> checkResultListSerde = new Serdes.ListSerde<>(ArrayList.class, AvroSerdes.<TxCheckResult>valueSerde(srConfig));

        var storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(TransactionAggregateProcessor.TRANSACTION_AGGREGATE_STORE_NAME), Serdes.String(), checkResultListSerde);
        builder.addStateStore(storeBuilder);

        var stream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), AvroSerdes.<Transaction>valueSerde(srConfig)));

        KStream<String, TxCheckResult> checkResultsStream = stream.process(txCheckResultProcessorSupplier, TransactionAggregateProcessor.TRANSACTION_AGGREGATE_STORE_NAME);

        BranchedKStream<String, TxCheckResult> checkResultBranchedKStream = checkResultsStream.split(Named.as(checkResultsSplit));
        checkResultBranchedKStream.branch((k, v) -> !v.getAnomalies().isEmpty(), Branched.as(anomaliesBranch));
        Map<String, KStream<String, TxCheckResult>> branchMap = checkResultBranchedKStream.defaultBranch(Branched.as(validBranch));

        KStream<String, TxCheckResult> anomaliesStream = branchMap.get(checkResultsSplit + anomaliesBranch);
        KStream<String, TxCheckResult> validTxCheckStream = branchMap.get(checkResultsSplit + validBranch);

        KStream<String, TxAnomaly> mappedValues = anomaliesStream.flatMapValues(TxCheckResult::getAnomalies);
        mappedValues.to(invalidTopic, Produced.with(Serdes.String(), AvroSerdes.<TxAnomaly>valueSerde(srConfig)));
        KStream<String, Transaction> validTxStream = validTxCheckStream.mapValues(TxCheckResult::getTransaction);
        validTxStream.to(validTopic, Produced.with(Serdes.String(), AvroSerdes.<Transaction>valueSerde(srConfig)));

        return builder.build();
    }
}
