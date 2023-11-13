package com.example.kstreams.fraud;

import com.example.kstreams.fraud.model.FinTransaction;
import com.example.kstreams.fraud.model.TxAndWindowedAmount;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
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

public class WindowedAmountLimitJsonEnrichedTopologyProducer {

    static int maxAmount = 100;
    static long windowDurationMs = 10000;
    static long graceDurationMs = 1000;

    static ObjectMapperSerde<FinTransaction> txSerde = new ObjectMapperSerde<>(FinTransaction.class);
    static ObjectMapperSerde<TxAndWindowedAmount> txAndAmountSerde = new ObjectMapperSerde<>(TxAndWindowedAmount.class);

    Logger log = Logger.getLogger(WindowedAmountLimitJsonEnrichedTopologyProducer.class);

    String sourceTopic;
    String validTopic;
    String invalidTopic;


    @Inject
    public WindowedAmountLimitJsonEnrichedTopologyProducer(@ConfigProperty(name = "sourceTopic") String sourceTopic, @ConfigProperty(name = "validTopic") String validTopic, @ConfigProperty(name = "invalidTopic") String invalidTopic) {
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
        Predicate<String, TxAndWindowedAmount> windowedTxVolumeTooHighPredicate = (k, v) -> v.amount() > maxAmount;

        WindowBytesStoreSupplier storeSupplier = Stores.persistentWindowStore("txAmountAggregationStore", windowDuration.plus(graceDuration), windowDuration, false);

        Materialized<String, TxAndWindowedAmount, WindowStore<Bytes, byte[]>> materialized = Materialized.as(storeSupplier);
        // implicitly a tumbling window - we do not define the advance in the factory method
        TimeWindows fraudCheckWindow = TimeWindows.ofSizeAndGrace(windowDuration, graceDuration);

        Produced<String, TxAndWindowedAmount> stringIntegerProduced = Produced.with(Serdes.String(), txAndAmountSerde);

        var builder = new StreamsBuilder();
        KStream<String, FinTransaction> txStream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), txSerde));

        KGroupedStream<String, FinTransaction> grouped = txStream.groupByKey();
        TimeWindowedKStream<String, FinTransaction> windowed = grouped.windowedBy(fraudCheckWindow);
        // initializer, aggregator, named, materialized
        Aggregator<String, FinTransaction, TxAndWindowedAmount> stringIntegerIntegerAggregator = (k, v, aggregate) -> aggregate.with().tx(v).amount(aggregate.amount() + v.amount()).build();
        KTable<Windowed<String>, TxAndWindowedAmount> txAmountAggregation = windowed.aggregate(TxAndWindowedAmount::empty, stringIntegerIntegerAggregator, Named.as("txAmountAggregation"), materialized.withKeySerde(Serdes.String()).withValueSerde(txAndAmountSerde));
        KStream<Windowed<String>, TxAndWindowedAmount> txAmountAggregationStream = txAmountAggregation.toStream();
        KStream<String, TxAndWindowedAmount> unwrappedKeyTxAggregationStream = txAmountAggregationStream.map((wk, value) -> KeyValue.pair(wk.key(), value));
        BranchedKStream<String, TxAndWindowedAmount> splitValidTransactionAggregations = unwrappedKeyTxAggregationStream.split(Named.as("splitValidTransactionAggregations"));

        splitValidTransactionAggregations.branch(windowedTxVolumeTooHighPredicate, Branched.withConsumer(ks -> ks.to(invalidTopic, stringIntegerProduced), "txOverLimitBranch"));
        splitValidTransactionAggregations.defaultBranch(Branched.withConsumer(ks -> ks.to(validTopic, stringIntegerProduced), "validTxBranch"));

        return builder.build();
    }
}
