package org.example.kafkatest2.kafka.worker;

import org.example.kafkatest2.KafkaTest2Application;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ThreadConsumerWorker {

    public static void main(String[] args) {
        SpringApplication.run(ThreadConsumerWorker.class, args);
    }



}
