package com.example.kstreams.anomaly.testdata;

import com.example.avro.Transaction;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Properties;

@ApplicationScoped
public class TxEmitter {

    Logger log = Logger.getLogger(TxEmitter.class);

    private final String broker;
    private final String schemaRegistryUrl;
    private final String topic;
    private KafkaProducer<String, Transaction> producer;

    private final Serde<Transaction> txSerde = new SpecificAvroSerde<>();

    private final TxGenerator txGenerator = new TxGenerator();

    public TxEmitter(    @ConfigProperty(name = "kafka.bootstrap.servers")
                         String broker,
    @ConfigProperty(name = "schema.registry.url")
    String schemaRegistryUrl,
    @ConfigProperty(name = "anomaly.sourceTopic")
    String topic) {
        this.broker = broker;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
    }

    @PostConstruct
    void setup() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

        Map<String, String> serdeConfig = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                                                 AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, "true"
        );
        txSerde.configure(serdeConfig, false);

        producer = new KafkaProducer<>(props, Serdes.String().serializer(), txSerde.serializer());
    }

    @Scheduled(every = "{txemitter.emit.interval}")
    public void emit() {
        var transaction = txGenerator.createTransaction();
        log.info("emitting " + transaction);
        ProducerRecord<String, Transaction> record = new ProducerRecord<>(topic, transaction.getAccountName(), transaction);
        producer.send(record);
    }
}
