package com.example.kstreams.anomaly.errorhandling;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.jboss.logging.Logger;

import java.util.Map;

public class DlqDeserializationHandler implements DeserializationExceptionHandler {
    KafkaProducer<byte[], byte[]> dlqProducer;
    String dlqTopic;

    Logger log = Logger.getLogger(DlqDeserializationHandler.class);

    @Override
    public DeserializationHandlerResponse handle(final ProcessorContext context,
                                                 final ConsumerRecord<byte[], byte[]> record,
                                                 final Exception exception) {

        log.warnv("DlqDeserializationHandler: exception at deserialization, sending to DLQ; " +
                          "taskId: {}, topic: {}, partition: {}, offset: {}, exception: {}",
                  context.taskId(), record.topic(), record.partition(), record.offset(),
                  exception.getMessage());

        dlqProducer.send(new ProducerRecord<>(dlqTopic, null, record.timestamp(), record.key(), record.value()));

        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(final Map<String, ?> configs) {

        Map<String, String>stringConfigs = (Map<String, String>) configs;
        dlqProducer = new KafkaProducer(configs, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());
        dlqTopic = stringConfigs.getOrDefault("default.deserialization.exception.handler.dlq.topic", "default_dlq_topic"); // get the topic name from the configs map
    }
}
