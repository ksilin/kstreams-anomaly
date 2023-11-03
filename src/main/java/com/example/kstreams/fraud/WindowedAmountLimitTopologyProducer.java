package com.example.kstreams.fraud;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;

public class WindowedAmountLimitTopologyProducer {

    static int maxAmount = 100;
    static long windowDurationMs = 10000;
    static long graceDurationMs = 1000;

    Logger log = Logger.getLogger(WindowedAmountLimitTopologyProducer.class);

    String sourceTopic;
    String validTopic;
    String invalidTopic;


    @Inject
    public WindowedAmountLimitTopologyProducer(@ConfigProperty(name = "sourceTopic") String sourceTopic, @ConfigProperty(name = "validTopic") String validTopic, @ConfigProperty(name = "invalidTopic") String invalidTopic) {
        this.sourceTopic = sourceTopic;
        this.validTopic = validTopic;
        this.invalidTopic = invalidTopic;
    }

    @Produces
    public Topology produceTopology() {
        return createTopology(this.sourceTopic, this.validTopic, this.invalidTopic);
    }


    public Topology createTopology(String sourceTopic, String validTopic, String invalidTopic) {

        Duration windowDuration = Duration.ofMillis(windowDurationMs);
        Duration graceDuration = Duration.ofMillis(graceDurationMs);
        Predicate<String, Integer> windowedTxVolumeTooHighPredicate = (k, v) -> v > maxAmount;

        WindowBytesStoreSupplier storeSupplier = Stores.persistentWindowStore("txAmountAggregationStore", windowDuration.plus(graceDuration), windowDuration, false);

        Materialized<String, Integer, WindowStore<Bytes, byte[]>> materialized = Materialized.as(storeSupplier);
        // implicitly a tumbling window - we do not define the advance in the factory method
        TimeWindows fraudCheckWindow = TimeWindows.ofSizeAndGrace(windowDuration, graceDuration);

        Produced<String, Integer> stringIntegerProduced = Produced.with(Serdes.String(), Serdes.Integer());

        var builder = new StreamsBuilder();
        KStream<String, Integer> txStream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.Integer()));

        KGroupedStream<String, Integer> grouped = txStream.groupByKey();
        TimeWindowedKStream<String, Integer> windowed = grouped.windowedBy(fraudCheckWindow);
        // initializer, aggregator, named, materialized
        Aggregator<String, Integer, Integer> stringIntegerIntegerAggregator = (k, v, aggregate) -> aggregate + v;
        KTable<Windowed<String>, Integer> txAmountAggregation = windowed.aggregate(() -> 0, stringIntegerIntegerAggregator, Named.as("txAmountAggregation"), materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.Integer()));
        KStream<Windowed<String>, Integer> txAmountAggregationStream = txAmountAggregation.toStream();
        KStream<String, Integer> unwrappedKeyTxAggregationStream = txAmountAggregationStream.map((wk, value) -> KeyValue.pair(wk.key(), value));
        BranchedKStream<String, Integer> splitValidTransactionAggregations = unwrappedKeyTxAggregationStream.split(Named.as("splitValidTransactionAggregations"));

        splitValidTransactionAggregations.branch(windowedTxVolumeTooHighPredicate, Branched.withConsumer(ks -> ks.to(invalidTopic, stringIntegerProduced), "txOverLimitBranch"));
        splitValidTransactionAggregations.defaultBranch(Branched.withConsumer(ks -> ks.to(validTopic, stringIntegerProduced), "validTxBranch"));

        return builder.build();
    }
}
