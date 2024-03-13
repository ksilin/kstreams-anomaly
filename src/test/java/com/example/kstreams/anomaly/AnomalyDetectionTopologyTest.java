package com.example.kstreams.anomaly;

import com.example.avro.Transaction;
import com.example.avro.TxAnomaly;
import com.example.avro.TxCheckResult;
import com.example.kstreams.anomaly.rules.SingleAmountAnomalyConfig;
import com.example.kstreams.anomaly.rules.WindowedAmountAnomalyConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class AnomalyDetectionTopologyTest {

    Logger log = Logger.getLogger(AnomalyDetectionTopologyTest.class);

    String sourceTopic = "testSourceTopic";
    String validTxTopic = "validTxTopic";
    String anomalyTopic = "anomalyTopic";

    static final Properties props = new Properties();

    // real SR
    static final String realSR = "http://localhost:8081";
    static final String mockSR = "mock://localhost:8081";

    static final Map<String, String> serdeConfig = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSR,
                                                          AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, "true"
    );

    @BeforeAll
    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AnomalyDetectionTopologyTest.class.getSimpleName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
        txSerde.configure(serdeConfig, false);
        txAnomalySerde.configure(serdeConfig, false);
        txCheckResultSerde.configure(serdeConfig, false);
    }

    static final Serde<Transaction> txSerde = new SpecificAvroSerde<>();
    static final Serde<TxAnomaly> txAnomalySerde = new SpecificAvroSerde<>();
    static final Serde<TxCheckResult> txCheckResultSerde = new SpecificAvroSerde<>();

    static final Serializer<String> stringSerializer = Serdes.String().serializer();
    static final Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    @Test
    void testSingleTxAmountLimit(){

        long storeRetentionMs = 10000;
        SingleAmountAnomalyConfig singleAmountAnomalyConfig = new SingleAmountAnomalyConfig(100);
        WindowedAmountAnomalyConfig windowedAmountAnomalyConfig = new WindowedAmountAnomalyConfig(200, 1000);
        ProcessorSupplier<String, Transaction, String, TxCheckResult> txCheckResultProcessorSupplier = () -> new TransactionAggregateProcessor(storeRetentionMs, singleAmountAnomalyConfig, windowedAmountAnomalyConfig);

        var topo = AnomalyDetectionTopologyProducer.createTopology(sourceTopic, validTxTopic, anomalyTopic, mockSR, txCheckResultProcessorSupplier);

        TestInputTopic<String, Transaction> inputTopic;
        TestOutputTopic<String, TxAnomaly> anomalyOutputTopic;
        TestOutputTopic<String, Transaction> validOutputTopic;

        try (var testDriver = new TopologyTestDriver(topo, props)) {

            inputTopic = testDriver.createInputTopic(sourceTopic, stringSerializer, txSerde.serializer());
            anomalyOutputTopic = testDriver.createOutputTopic(anomalyTopic, stringDeserializer, txAnomalySerde.deserializer());
            validOutputTopic = testDriver.createOutputTopic(validTxTopic, stringDeserializer, txSerde.deserializer());

            long now = Instant.now().toEpochMilli();
            String accountName = "singleAccount";
            int underLimitAmount = 99;
            int overLimitAmount = 101;
            String underLimitTxId = "1";
            String overLimitTxId = "2";
            double lat = 1.0;
            double lon = 1.0;
            Transaction validTx = new Transaction(accountName, underLimitAmount, underLimitTxId, lat, lon, now);
            Transaction txOverSingleLimit = new Transaction(accountName, overLimitAmount, overLimitTxId, lat, lon, now);

            inputTopic.pipeInput(accountName, validTx);
            inputTopic.pipeInput(accountName, txOverSingleLimit);

            List<Transaction> validResultsSingleTx = validOutputTopic.readValuesToList();
            List<TxAnomaly> anomalyResultsSingleTx = anomalyOutputTopic.readValuesToList();

            assertThat(validResultsSingleTx).hasSize(1);
            assertThat(anomalyResultsSingleTx).hasSize(1);

            String windowedAccountName = "windowedAccount";
            String txId1 = "10";
            String txId2 = "20";
            String overWindowLimitTxId = "30";
            String fillterTxId = "40";
            String overLimitTxId2 = "50";
            Transaction tx1 = new Transaction(windowedAccountName, underLimitAmount, txId1, lat, lon, now);
            Transaction tx2 = new Transaction(windowedAccountName, underLimitAmount, txId2, lat, lon, now);

            inputTopic.pipeInput(windowedAccountName, tx1);
            inputTopic.pipeInput(windowedAccountName, tx2);

            KeyValueStore<String, List<Transaction>> store = testDriver.getKeyValueStore(TransactionAggregateProcessor.TRANSACTION_AGGREGATE_STORE_NAME);

            testDriver.advanceWallClockTime(Duration.ofMillis(10000));
            Transaction overLimitButTooLate = new Transaction(windowedAccountName, underLimitAmount, overWindowLimitTxId, lat, lon, now + 10000);


            Transaction fillerToGetNextOneOverWindowLimit = new Transaction(windowedAccountName, underLimitAmount, fillterTxId, lat, lon, now + 10000);
            Transaction overLimitInNewWindow = new Transaction(windowedAccountName, underLimitAmount, overLimitTxId2, lat, lon, now + 10000);

            assertThat(store.get(windowedAccountName)).isNotNull().hasSize(2);

            // this is where housekeeping happens
            inputTopic.pipeInput(windowedAccountName, overLimitButTooLate);

            assertThat(store.get(windowedAccountName)).isNotNull().hasSize(1);

            inputTopic.pipeInput(windowedAccountName, fillerToGetNextOneOverWindowLimit);
            inputTopic.pipeInput(windowedAccountName, overLimitInNewWindow);


            List<Transaction> validResults = validOutputTopic.readValuesToList();
            List<TxAnomaly> anomalyResults = anomalyOutputTopic.readValuesToList();
            //results.forEach(s -> log.infof("result: %s", s));

            assertThat(validResults).hasSize(4);
            assertThat(anomalyResults).hasSize(1);
        }
    }
}
