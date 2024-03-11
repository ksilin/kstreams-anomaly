package com.example.kstreams.anomaly;

import com.example.avro.Transaction;
import com.example.avro.TxAnomaly;
import com.example.avro.TxCheckResult;
import com.example.kstreams.anomaly.rules.SingleAmountAnomalyCheck;
import com.example.kstreams.anomaly.rules.SingleAmountAnomalyConfig;
import com.example.kstreams.anomaly.rules.WindowedAmountAnomalyCheck;
import com.example.kstreams.anomaly.rules.WindowedAmountAnomalyConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
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

public class TransactionAggregateProcessorTest {

    Logger log = Logger.getLogger(TransactionAggregateProcessorTest.class);

    String sourceTopic = "testSourceTopic";
    String resultTopic = "testResultTopic";

    static final Properties props = new Properties();

    // real SR
    static final String realSR = "http://localhost:8081";
    static final String mockSR = "mock://localhost:8081";

    static final Map<String, String> serdeConfig = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSR,
                                                          AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, "true"
    );

    @BeforeAll
    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, TransactionAggregateProcessorTest.class.getSimpleName());
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

        var topo = createTestTopology(txSerde);
        TestInputTopic<String, Transaction> inputTopic;
        TestOutputTopic<String, TxCheckResult> resultOutputTopic;
        try (var testDriver = new TopologyTestDriver(topo, props)) {

            inputTopic = testDriver.createInputTopic(sourceTopic, stringSerializer, txSerde.serializer());
            resultOutputTopic = testDriver.createOutputTopic(resultTopic, stringDeserializer, txCheckResultSerde.deserializer());

            long now = Instant.now().toEpochMilli();
            String accountName = "testAccount";
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

            List<TxCheckResult> results = resultOutputTopic.readValuesToList();
            //results.forEach(s -> log.infof("result: %s", s));

            assertThat(results).hasSize(2);

            assertThat(results.get(0).getAnomalies()).isEmpty();
            TxCheckResult overSingleLimitResult = results.get(1);
            assertThat(overSingleLimitResult.getAnomalies()).hasSize(1);
            TxAnomaly anomaly = overSingleLimitResult.getAnomalies().get(0);
            assertThat(anomaly.getAnomalyType()).isEqualTo(SingleAmountAnomalyCheck.SINGLE_TX_AMOUNT_LIMIT_ANOMALY);
            //overSingleLimitResult.getAnomalies().forEach(a -> log.infof("anomaly: %s", a));
        }
    }

    @Test
    void testWindowedAmountLimit(){

        var topo = createTestTopology(txSerde);
        TestInputTopic<String, Transaction> inputTopic;
        TestOutputTopic<String, TxCheckResult> resultOutputTopic;
        try (var testDriver = new TopologyTestDriver(topo, props)) {

            inputTopic = testDriver.createInputTopic(sourceTopic, stringSerializer, txSerde.serializer());
            resultOutputTopic = testDriver.createOutputTopic(resultTopic, stringDeserializer, txCheckResultSerde.deserializer());

            long now = Instant.now().toEpochMilli();
            String accountName = "testAccount";
            int amount = 99;
            String txId1 = "1";
            String txId2 = "2";
            String overLimitTxId = "3";
            double lat = 1.0;
            double lon = 1.0;
            Transaction tx1 = new Transaction(accountName, amount, txId1, lat, lon, now);
            Transaction tx2 = new Transaction(accountName, amount, txId2, lat, lon, now);
            Transaction tx3 = new Transaction(accountName, amount, overLimitTxId, lat, lon, now);

            // TODO - also test housekeeping of windowed state

            inputTopic.pipeInput(accountName, tx1);
            inputTopic.pipeInput(accountName, tx2);
            inputTopic.pipeInput(accountName, tx3);

            List<TxCheckResult> results = resultOutputTopic.readValuesToList();
            //results.forEach(s -> log.infof("result: %s", s));

            assertThat(results).hasSize(3);

            assertThat(results.get(0).getAnomalies()).isEmpty();
            assertThat(results.get(1).getAnomalies()).isEmpty();
            TxCheckResult overWindowedLimitResult = results.get(2);
            assertThat(overWindowedLimitResult.getAnomalies()).hasSize(1);
            TxAnomaly anomaly = overWindowedLimitResult.getAnomalies().get(0);
            assertThat(anomaly.getAnomalyType()).isEqualTo(WindowedAmountAnomalyCheck.WINDOWED_AGGREGATED_TX_AMOUNT_LIMIT_ANOMALY);
            //overWindowedLimitResult.getAnomalies().forEach(a -> log.infof("anomaly: %s", a));
        }
    }

    @Test
    void testWindowedAmountLimitWithStoreCleanup(){

        var topo = createTestTopology(txSerde);
        TestInputTopic<String, Transaction> inputTopic;
        TestOutputTopic<String, TxCheckResult> resultOutputTopic;
        try (var testDriver = new TopologyTestDriver(topo, props)) {

            inputTopic = testDriver.createInputTopic(sourceTopic, stringSerializer, txSerde.serializer());
            resultOutputTopic = testDriver.createOutputTopic(resultTopic, stringDeserializer, txCheckResultSerde.deserializer());

            long now = Instant.now().toEpochMilli();
            String accountName = "testAccount";
            int amount = 99;
            String txId1 = "1";
            String txId2 = "2";
            String overLimitTxId = "3";
            String fillterTxId = "4";
            String overLimitTxId2 = "5";
            double lat = 1.0;
            double lon = 1.0;
            Transaction tx1 = new Transaction(accountName, amount, txId1, lat, lon, now);
            Transaction tx2 = new Transaction(accountName, amount, txId2, lat, lon, now);

            KeyValueStore<String, List<Transaction>> store = testDriver.getKeyValueStore(TransactionAggregateProcessor.TRANSACTION_AGGREGATE_STORE_NAME);


            testDriver.advanceWallClockTime(Duration.ofMillis(10000));
            Transaction overLimitButTooLate = new Transaction(accountName, amount, overLimitTxId, lat, lon, now + 10000);


            Transaction fillerToGetNextOneOverWindowLimit = new Transaction(accountName, amount, fillterTxId, lat, lon, now + 10000);
            Transaction overLimitInNewWindow = new Transaction(accountName, amount, overLimitTxId2, lat, lon, now + 10000);

            inputTopic.pipeInput(accountName, tx1);
            inputTopic.pipeInput(accountName, tx2);

            assertThat(store.get(accountName)).isNotNull().hasSize(2);

            // this is where housekeeping happens
            inputTopic.pipeInput(accountName, overLimitButTooLate);

            assertThat(store.get(accountName)).isNotNull().hasSize(1);

            inputTopic.pipeInput(accountName, fillerToGetNextOneOverWindowLimit);
            inputTopic.pipeInput(accountName, overLimitInNewWindow);

            List<TxCheckResult> results = resultOutputTopic.readValuesToList();
            //results.forEach(s -> log.infof("result: %s", s));

            assertThat(results).hasSize(5);

            assertThat(results.get(0).getAnomalies()).isEmpty();
            assertThat(results.get(1).getAnomalies()).isEmpty();
            assertThat(results.get(2).getAnomalies()).isEmpty();
            assertThat(results.get(3).getAnomalies()).isEmpty();
            TxCheckResult overWindowedLimitResult = results.get(4);
            assertThat(overWindowedLimitResult.getAnomalies()).hasSize(1);
            TxAnomaly anomaly = overWindowedLimitResult.getAnomalies().get(0);
            assertThat(anomaly.getAnomalyType()).isEqualTo(WindowedAmountAnomalyCheck.WINDOWED_AGGREGATED_TX_AMOUNT_LIMIT_ANOMALY);
            //overWindowedLimitResult.getAnomalies().forEach(a -> log.infof("anomaly: %s", a));
        }
    }


    Topology createTestTopology(Serde<Transaction> txSerde){
        var builder = new StreamsBuilder();

        long storeRetentionMs = 10000;
        SingleAmountAnomalyConfig singleAmountAnomalyConfig = new SingleAmountAnomalyConfig(100);
        WindowedAmountAnomalyConfig windowedAmountAnomalyConfig = new WindowedAmountAnomalyConfig(200, 1000);

        Serdes.ListSerde<TxCheckResult> listSerde = new Serdes.ListSerde<>(ArrayList.class, txCheckResultSerde);

        var storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(TransactionAggregateProcessor.TRANSACTION_AGGREGATE_STORE_NAME), Serdes.String(), listSerde);
        builder.addStateStore(storeBuilder);

        var stream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), txSerde));
        stream.foreach((k,v) -> log.infof("k: %s, v: %s", k, v));
        stream.process(() -> new TransactionAggregateProcessor(storeRetentionMs, singleAmountAnomalyConfig, windowedAmountAnomalyConfig), TransactionAggregateProcessor.TRANSACTION_AGGREGATE_STORE_NAME)
        .to(resultTopic, Produced.with(Serdes.String(), txCheckResultSerde));
        return builder.build();
    }

}
