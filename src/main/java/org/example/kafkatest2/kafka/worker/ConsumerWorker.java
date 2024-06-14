package org.example.kafkatest2.kafka.worker;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWorker implements Runnable{
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties prop, String topic, int number)
    {
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Arrays.asList(topic));
        while(true){
            logger.info("running thread : " + threadName);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            records.forEach(record -> {
                logger.info("thread_inner: {} , recordValue: {}",Thread.currentThread().getName(),record.value());
            });
        }

    }

}
