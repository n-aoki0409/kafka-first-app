package com.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class FirstAppProducer {
    private static final String topicName = "first-topic";

    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "broker1:29092,broker2:29092,broker3:29092");
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = new KafkaProducer<>(conf);

        int key;
        String value;

        for(int i = 1; i <= 100; i++) {
            key = i;
            value = String.valueOf(i);

            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, key, value);
            producer.send(record, (metadata, e) -> {
                if(metadata != null) {
                    String infoString = String.format("Success partition: %d, offset: %d", metadata.partition(), metadata.offset());
                    System.out.println(infoString);
                } else {
                    String infoString = String.format("Failed: %s", e.getMessage());
                    System.err.println(infoString);
                }
            });
        }

        producer.close();
    }
}
