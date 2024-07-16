package com.example.kstreams.anomaly.testdata;

import com.example.avro.Transaction;
import com.example.kstreams.anomaly.util.AvroSerdes;
import com.example.kstreams.anomaly.util.SrConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

@ApplicationScoped
public class TxEmitter {

    Logger log = Logger.getLogger(TxEmitter.class);

    private final String broker;
    private final SrConfig srConfig;
    private final String topic;

    private final String saslMechanism;
    private final String securityProtocol;
    private final Optional<String> saslConfig;
    private KafkaProducer<String, Transaction> producer;

    private final TxGenerator txGenerator = new TxGenerator();

    @Inject
    public TxEmitter(@ConfigProperty(name = "kafka.bootstrap.servers")
                     String broker,
                     @ConfigProperty(name = "anomaly.sourceTopic")
                     String topic,
                     @ConfigProperty(name = "kafka.sasl.mechanism", defaultValue = "PLAIN")
                     String saslMechanism,
                     @ConfigProperty(name = "kafka.security.protocol", defaultValue = "PLAINTEXT")
                     String securityProtocol,
                     @ConfigProperty(name = "kafka.sasl.jaas.config", defaultValue = "")
                         Optional<String> saslConfig,
           SrConfig srConfig
    ) {
        this.broker = broker;
        this.topic = topic;
        this.securityProtocol = securityProtocol;
        this.saslMechanism = saslMechanism;
        this.saslConfig = saslConfig;
        this.srConfig = srConfig;
    }

    @PostConstruct
    void setup() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);

        saslConfig.ifPresent(s -> props.put(SaslConfigs.SASL_JAAS_CONFIG, s));

        Serde<Transaction> txSerde = AvroSerdes.valueSerde(srConfig);
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
