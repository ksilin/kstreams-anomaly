package com.example.kstreams.fraud;

import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@Startup
@ApplicationScoped
public class StatelessTopologyProducer {

    Logger log = Logger.getLogger(StatelessTopologyProducer.class);

    String sourceTopic;
    String validTopic;
    String invalidTopic;

    @Inject
    public StatelessTopologyProducer(@ConfigProperty(name = "sourceTopic") String sourceTopic, @ConfigProperty(name = "validTopic") String validTopic, @ConfigProperty(name = "invalidTopic") String invalidTopic) {
        this.sourceTopic = sourceTopic;
        this.validTopic = validTopic;
        this.invalidTopic = invalidTopic;
    }

    @Produces
    public Topology produceTopology() {
        return createTopology(this.sourceTopic, this.validTopic, this.invalidTopic);
    }

    public Topology createTopology(String sourceTopic, String validTopic, String invalidTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        Predicate<? super String, ? super String> invalidPredicate = (k, v) -> v.contains("boo");

        builder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> log.infov("validating message {0}:{1}, k, v"))
                .split(Named.as("validationSplit"))
                .branch(invalidPredicate, Branched.withConsumer(ks -> ks.to(invalidTopic)))
                .defaultBranch(Branched.withConsumer(ks -> ks.to(validTopic)));

        return builder.build();
    }


}
