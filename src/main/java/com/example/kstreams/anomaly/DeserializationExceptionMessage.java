package com.example.kstreams.anomaly;

public record DeserializationExceptionMessage(String task, String topic, int partition, long offset, String exceptionMessage, byte[] key, byte[] value) {

}
