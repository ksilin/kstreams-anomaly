package com.example.kstreams.anomaly.util;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import java.util.HashMap;

public class AvroSerdes {

    public static <T extends SpecificRecord> SpecificAvroSerde<T> valueSerde(SrConfig srConfig) {
        var specificSerdes = new SpecificAvroSerde<T>();

        var propMap = new HashMap<String, Object>();
        propMap.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srConfig.srUrl);
        propMap.put(AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, "true");
        if(srConfig.basicAuthUserInfo.isPresent()) {
            propMap.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            propMap.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srConfig.basicAuthUserInfo.get());
        }
        specificSerdes.configure(propMap, false);
        return specificSerdes;
    }

}
