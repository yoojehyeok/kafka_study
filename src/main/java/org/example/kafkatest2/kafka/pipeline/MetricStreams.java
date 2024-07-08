package org.example.kafkatest2.kafka.pipeline;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.eclipse.jetty.util.thread.ShutdownThread;

import java.util.Properties;

public class MetricStreams {
    private static KafkaStreams streams;

    public static void metricStreamsStart() {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "metric-streams-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "125.181.184.135:2116");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> metrics = builder.stream("metricAll");
        metrics.foreach((key, value) -> System.out.println("key1: " + key + ", value1: " + value));
        KStream<String, String>[] metricBranch = metrics.branch(
                (key, value) -> MetricJsonUtils.getMetricName(value).equals("cpu"),
                (key, value) -> MetricJsonUtils.getMetricName(value).equals("memory")
        );

        System.out.println("!!!" + metricBranch[0].toString());
        System.out.println(metricBranch[0].toString());

        System.out.println("!!!" + metricBranch[1].toString());

        metricBranch[0].to("metricCpu");
        metricBranch[1].to("metricMemory");

        KStream<String, String> filteredCpuMetric = metricBranch[0]
                .filter((key, value) -> MetricJsonUtils.getTotalCpuPercent(value) > 0.5);

        filteredCpuMetric.mapValues(value -> MetricJsonUtils.getHostTimestamp(value)).to("metricCpuAlert");

        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    static class ShutdownThread extends Thread {
        public void run() {
            streams.close();
        }
    }
}