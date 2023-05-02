package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Collections;
import java.util.Properties;

public class Streaming {
    public static void main(final String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-example-1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:29092,broker2:29092,broker3:29092");

        ObjectMapper mapper = new ObjectMapper();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> metrics = builder.stream("kafka.metrics", Consumed.with(Serdes.String(), Serdes.String()));

        metrics
            .flatMapValues(wrap(mapper::readTree))
            .filter((host, root) -> root.has("value") && root.has("hostname") && root.has("timestamp"))
            .flatMapValues(wrap(root -> {
                ObjectNode newroot = mapper.createObjectNode();
                newroot.put("hostname", root.get("hostname"));
                newroot.put("timestamp", root.get("timestamp"));
                newroot.put("BytesIn", root.get("value").get("kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics").get("Count"));
                return mapper.writeValueAsString(newroot);
            }))
            .to("kafka.metrics.processed.1", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    @FunctionalInterface
    private interface FunctionWithException<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    private static<V, VR, E extends Exception> ValueMapper<V, Iterable<VR>> wrap(FunctionWithException<V, VR, E> f) {
        return new ValueMapper<V, Iterable<VR>>() {
            public Iterable<VR> apply(V v) {
                try {
                    return Collections.singletonList(f.apply(v));
                } catch (Exception e) {
                    e.printStackTrace();
                    return Collections.emptyList();
                }
            }
        };
    }
}
