package com.example.kstreams.fraud;

import com.example.kstreams.fraud.model.FinTransaction;
import com.example.kstreams.fraud.model.TxAndWindowedAmount;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class StatefulJsonTopologyTest {

    Logger log = Logger.getLogger(StatefulJsonTopologyTest.class);

    String sourceTopic = "testSourceTopic";
    String validTopic = "testValidTopic";
    String invalidTopic = "testInvalidTopic";

    static final Properties props = new Properties();

    WindowedAmountLimitJsonTopologyProducer topologyProducer = new WindowedAmountLimitJsonTopologyProducer(sourceTopic, validTopic, invalidTopic);

    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, StatefulJsonTopologyTest.class.getSimpleName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    static final Serializer<String> stringSerializer = Serdes.String().serializer();
    static final Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    static final Serde<FinTransaction> txSerde = WindowedAmountLimitJsonTopologyProducer.txSerde;
    static final Serializer<FinTransaction> txSerializer = txSerde.serializer();
    static final Deserializer<FinTransaction> txDeserializer = txSerde.deserializer();

    static final Serde<TxAndWindowedAmount> txAndAmountSerde = WindowedAmountLimitJsonTopologyProducer.txAndAmountSerde;
    static final Serializer<TxAndWindowedAmount> txAndAmountSerializer = txAndAmountSerde.serializer();
    static final Deserializer<TxAndWindowedAmount> txAndAmountDeserializer = txAndAmountSerde.deserializer();


    @Test
    void testValidateAndRouteMessages(){

        var topo = topologyProducer.produceTopology();
        var testDriver = new TopologyTestDriver(topo, props);


        var inputTopic = testDriver.createInputTopic(sourceTopic, stringSerializer, txSerializer);

        var validOutputTopic = testDriver.createOutputTopic(validTopic, stringDeserializer, txAndAmountDeserializer);
        var invalidOutputTopic = testDriver.createOutputTopic(invalidTopic, stringDeserializer, txAndAmountDeserializer);

        var validTx = new FinTransaction("2", "2", 99, "id1", 0);
        var invalidTx = new FinTransaction("1", "2", 120, "id1", 0);

        var validTxAndAmount = new TxAndWindowedAmount(validTx, validTx.amount());
        var invalidTxAndAmount = new TxAndWindowedAmount(invalidTx, invalidTx.amount());

        inputTopic.pipeInput("1", invalidTx);
        inputTopic.pipeInput("2", validTx);

        var validMessages = validOutputTopic.readValuesToList();
        var invalidMessages = invalidOutputTopic.readValuesToList();

        assertThat(validMessages).hasSize(1).contains(validTxAndAmount);
        assertThat(invalidMessages).hasSize(1).contains(invalidTxAndAmount);
    }


}
