package com.example.kstreams.anomaly.testdata;

import com.example.avro.Transaction;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
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
import java.util.Properties;

@ApplicationScoped
public class TxEmitter {

    Logger log = Logger.getLogger(TxEmitter.class);

    private final String broker;
    private final String schemaRegistryUrl;
    private final String topic;

    private final String saslMechanism;
    private final String securityProtocol;
    private final String saslConfig;
    private final String basicAuthUserInfo;
    private KafkaProducer<String, Transaction> producer;

    private final Serde<Transaction> txSerde = new SpecificAvroSerde<>();

    private final TxGenerator txGenerator = new TxGenerator();

    public TxEmitter(@ConfigProperty(name = "kafka.bootstrap.servers")
                     String broker,
                     @ConfigProperty(name = "schema.registry.url")
                     String schemaRegistryUrl,
                     @ConfigProperty(name = "anomaly.sourceTopic")
                     String topic,
                     @ConfigProperty(name = "kafka.sasl.mechanism", defaultValue = "")
                     String saslMechanism,
                     @ConfigProperty(name = "kafka.security.protocol", defaultValue = "PLAINTEXT")
                     String securityProtocol,
                     @ConfigProperty(name = "kafka.sasl.jaas.config", defaultValue = "")
                     String saslConfig,
                     @ConfigProperty(name = "basic.auth.user.info", defaultValue = "") String basicAuthUserInfo
    ) {
        this.broker = broker;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
        this.securityProtocol = securityProtocol;
        this.saslMechanism = saslMechanism;
        this.saslConfig = saslConfig;
        this.basicAuthUserInfo = basicAuthUserInfo;
    }

    @PostConstruct
    void setup() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslConfig);

        Map<String, String> serdeConfig = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                                                 AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, "true",
                                                 AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO",
                                                 AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, basicAuthUserInfo
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
