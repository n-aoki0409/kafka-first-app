package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class EnrichmentStreaming {
    private static final String sensorDataTopicName = "mqtt-source";
    private static final String masterDataTopicName = "device-master";
    private static final String enrichedTopicName = "mqtt-enriched";

    public static void main(final String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-enrichment");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:29092,broker2:29092,broker3:29092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> mqttSource = builder.stream(sensorDataTopicName, Consumed.with(Serdes.String(), Serdes.String()));
        GlobalKTable<String, String> deviceMasterTable = builder.globalTable(masterDataTopicName, Consumed.with(Serdes.String(), Serdes.String()));

        mqttSource
            .map((key, value) -> KeyValue.pair(value.split(",")[0], value))
            .leftJoin(deviceMasterTable, (sensorKey, sensorValue) -> sensorKey, EnrichmentStreaming::SensorDataJoiner)
            .to(enrichedTopicName, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    private static String SensorDataJoiner(String sensorValue, String masterValue) {
        StringBuilder sb = new StringBuilder();
        sb.append(sensorValue.split(",", 2)[1]);
        sb.append(",");
        sb.append(masterValue);

        return new String(sb);
    }
}
