package org.example.kafkatest2.kafka.filter;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class SimpleFilter {

    @Value("${kafka.bootstrap-server}")
    String bootstrapServer;
    private final static String APLICATION_NAME = "filter-application";

    private final static String STREAM_LOG = "stream-log";

    private final static String TOPIC = "test";
    public void filterData(){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props. put (StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream (TOPIC);

        streamLog.filter((key, value) -> value.length() > 5).to(STREAM_LOG);

        KafkaStreams streams = new KafkaStreams (builder.build(), props) ;

        streams.start();
    }
}
