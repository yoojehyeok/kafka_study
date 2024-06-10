package org.example.kafkatest2.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.kafkatest2.kafka.worker.ConsumerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Component
public class SimpleConsumer {

    @Value("${kafka.bootstrap-server}")
    String BOOTSTRAP_SERVERS_CONFIG;
    private static boolean consumeFlag = false;
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String GROUP_ID = "testGroup";
    private static Properties configs = new Properties();
    private static KafkaConsumer<String, String> consumer;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public String consumeStart(String topicName) {
//        consumer.assign(Collections.singleton((new TopicPartition(TopicName, 0))));

        if(consumeFlag) {
            return "Already Consuming";
        }else{
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown Hook is running");
                consumer.wakeup();
            }));
            consumeFlag = true;
        }
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS_CONFIG);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(configs);

        consumer.subscribe(Arrays.asList(topicName), new RebalancerListener());

        consume();
        return "Consuming Started";

    }

    public void consume(){
        try{
            ExecutorService executorService = Executors.newCachedThreadPool();
            logger.info("consume start");
            while(consumeFlag){

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                 logger.info("thread_poll: {} ,",records.count());
                 for(ConsumerRecord<String, String> record :records){
//                    logger.info("{}", record);
                    logger.info("thread: {} ,",records);

                    ConsumerWorker consumerWorker = new ConsumerWorker(record.value());
                    executorService.execute(consumerWorker);
//                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1, null));
//                    consumer.commitSync(currentOffsets);
                }
            }
        }catch(WakeupException e) {
            logger.info("Wakeup Exception");
            consumer.close();
        }
    }

    public void consumeStop(){
        consumeFlag = false;
        consumer.close();
    }



    private static class RebalancerListener implements ConsumerRebalanceListener{
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            logger.info("Partitions are Revoked");
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            logger.info("Partitions are Assigned");
        }
    }
}
















