package com.example.kstreams.anomaly;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class StatefulTopologyTest {

    Logger log = Logger.getLogger(StatefulTopologyTest.class);

    String sourceTopic = "testSourceTopic";
    String validTopic = "testValidTopic";
    String invalidTopic = "testInvalidTopic";

    static final Properties props = new Properties();

    WindowedAmountLimitTopologyProducer topologyProducer = new WindowedAmountLimitTopologyProducer(sourceTopic, validTopic, invalidTopic);

    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, StatefulTopologyTest.class.getSimpleName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    static final Serializer<String> stringSerializer = Serdes.String().serializer();
    static final Serializer<Integer> integerSerializer = Serdes.Integer().serializer();
    static final Deserializer<String> stringDeserializer = Serdes.String().deserializer();
    static final Deserializer<Integer> integerDeserializer = Serdes.Integer().deserializer();

    @Test
    void testValidateAndRouteMessages(){

        var topo = topologyProducer.produceTopology();
        var testDriver = new TopologyTestDriver(topo, props);


        var inputTopic = testDriver.createInputTopic(sourceTopic, stringSerializer, integerSerializer);

        var validOutputTopic = testDriver.createOutputTopic(validTopic, stringDeserializer, integerDeserializer);
        var invalidOutputTopic = testDriver.createOutputTopic(invalidTopic, stringDeserializer, integerDeserializer);

        inputTopic.pipeInput("1", 120);
        inputTopic.pipeInput("2", 99);

        var validMessages = validOutputTopic.readValuesToList();
        var invalidMessages = invalidOutputTopic.readValuesToList();

        assertThat(validMessages).hasSize(1).contains(99);
        assertThat(invalidMessages).hasSize(1).contains(120);
    }


}
