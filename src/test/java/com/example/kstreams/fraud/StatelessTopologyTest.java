package com.example.kstreams.fraud;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

public class StatelessTopologyTest {

    Logger log = Logger.getLogger(StatelessTopologyTest.class);

    String sourceTopic = "testSourceTopic";
    String validTopic = "testValidTopic";
    String invalidTopic = "testInvalidTopic";

    static final Properties props = new Properties();

    StatelessTopologyProducer statelessTopologyProducer = new StatelessTopologyProducer(sourceTopic, validTopic, invalidTopic);

    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, StatelessTopologyTest.class.getSimpleName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    static final Serializer<String> stringSerializer = Serdes.String().serializer();
    static final Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    @Test
    void testValidateAndRouteMessages(){

        var topo = statelessTopologyProducer.produceTopology();
        var testDriver = new TopologyTestDriver(topo, props);


        var inputTopic = testDriver.createInputTopic(sourceTopic, stringSerializer, stringSerializer);

        var validOutputTopic = testDriver.createOutputTopic(validTopic, stringDeserializer, stringDeserializer);
        var invalidOutputTopic = testDriver.createOutputTopic(invalidTopic, stringDeserializer, stringDeserializer);

        String validMessage = "validMessage";
        inputTopic.pipeInput("1", validMessage);
        String invalidMessage = "invalidMessage boo!";
        inputTopic.pipeInput("2", invalidMessage);

        var validMessages = validOutputTopic.readValuesToList();
        var invalidMessages = invalidOutputTopic.readValuesToList();

        assertThat(validMessages).hasSize(1).contains(validMessage);
        assertThat(invalidMessages).hasSize(1).contains(invalidMessage);
    }


}
