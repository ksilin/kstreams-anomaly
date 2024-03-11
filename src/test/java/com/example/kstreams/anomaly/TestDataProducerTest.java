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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDataProducerTest {

    Logger log = Logger.getLogger(TestDataProducerTest.class);

    String txInTopic = "txIn";
    static final Properties props = new Properties();

    // real SR
    static final String realSR = "http://localhost:8081";

    static final Map<String, String> serdeConfig = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, realSR,
                                                          AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, "true"
    );

    @BeforeAll
    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, TestDataProducerTest.class.getSimpleName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:39092");
        txSerde.configure(serdeConfig, false);
    }

    static final Serde<Transaction> txSerde = new SpecificAvroSerde<>();
    static final Serializer<String> stringSerializer = Serdes.String().serializer();

    @Test
    void testSingleTxAmountLimit() throws ExecutionException, InterruptedException, TimeoutException {


        var producer = new KafkaProducer<String, Transaction>(props, stringSerializer, txSerde.serializer());

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

            String windowedAccountName = "windowedAccount";
            String txId1 = "10";
            String txId2 = "20";
            String overWindowLimitTxId = "30";
            String fillerTxId = "40";
            String overLimitTxId2 = "50";
            Transaction tx1 = new Transaction(windowedAccountName, underLimitAmount, txId1, lat, lon, now);
            Transaction tx2 = new Transaction(windowedAccountName, underLimitAmount, txId2, lat, lon, now);

            Transaction overLimitButTooLate = new Transaction(windowedAccountName, underLimitAmount, overWindowLimitTxId, lat, lon, now + 10000);
            Transaction fillerToGetNextOneOverWindowLimit = new Transaction(windowedAccountName, underLimitAmount, fillerTxId, lat, lon, now + 10000);
            Transaction overLimitInNewWindow = new Transaction(windowedAccountName, underLimitAmount, overLimitTxId2, lat, lon, now + 10000);

            producer.send(new ProducerRecord<>(txInTopic, validTx.getAccountName(), validTx));
        producer.flush();
            producer.send(new ProducerRecord<>(txInTopic, txOverSingleLimit.getAccountName(), txOverSingleLimit));
            producer.send(new ProducerRecord<>(txInTopic, tx1.getAccountName(), tx1));
            producer.send(new ProducerRecord<>(txInTopic, tx2.getAccountName(), tx2));
            producer.send(new ProducerRecord<>(txInTopic, overLimitButTooLate.getAccountName(), overLimitButTooLate));
            producer.send(new ProducerRecord<>(txInTopic, fillerToGetNextOneOverWindowLimit.getAccountName(), fillerToGetNextOneOverWindowLimit));
            producer.send(new ProducerRecord<>(txInTopic, overLimitInNewWindow.getAccountName(), overLimitInNewWindow));
            producer.flush();
            producer.close();
    }

}
