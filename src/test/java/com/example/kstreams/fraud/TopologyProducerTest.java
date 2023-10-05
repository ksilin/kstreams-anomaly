package com.example.kstreams.fraud;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class TopologyProducerTest {

    Logger log = Logger.getLogger(TopologyProducerTest.class);

    String sourceTopic = "testSourceTopic";
    String validTopic = "testValidTopic";
    String invalidTopic = "testInalidTopic";

    static final Properties props = new Properties();

    StatelessTopologyProducer statelessTopologyProducer = new StatelessTopologyProducer(sourceTopic, validTopic, invalidTopic);

    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, TopologyProducerTest.class.getSimpleName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }


    @Test
    void testValidateAndRoute(){

        var topo = statelessTopologyProducer.produceTopology();
        var testDriver = new TopologyTestDriver(topo, props);

        var inputTopic = testDriver.createInputTopic(sourceTopic, Serdes.String().serializer(), Serdes.String().serializer());
        var validOutputTopic = testDriver.createOutputTopic(validTopic, Serdes.String().deserializer(), Serdes.String().deserializer());
        var invalidOutputTopic = testDriver.createOutputTopic(invalidTopic, Serdes.String().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("1", "sdfasojfw");
        inputTopic.pipeInput("2", "boo");

        var validMessages = validOutputTopic.readValuesToList();
        var invalidMessages = invalidOutputTopic.readValuesToList();

        System.out.println("valid messages: ");
        validMessages.forEach(System.out::println);
        System.out.println("invalid messages: ");
        invalidMessages.forEach(System.out::println);


    }


}
