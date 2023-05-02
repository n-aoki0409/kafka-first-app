package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class FirstAppConsumer {
    private static final String topicName = "first-topic";

    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "broker1:29092,broker2:29092,broker3:29092");
        conf.setProperty("group.id", "FirstAppConsumerGroup");
        conf.setProperty("enable.auto.commit", "false");
        conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<Integer, String> consumer = new KafkaConsumer<>(conf);

        List<String> topicList = new ArrayList<>(1);
        topicList.add(topicName);
        consumer.subscribe(topicList);

        for(int count = 0; count < 300; count++) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<Integer, String> record: records) {
                String msgString = String.format("key: %d, value: %s", record.key(), record.value());
                System.out.println(msgString);

                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
                Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
                consumer.commitSync(commitInfo);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        consumer.close();
    }
}
