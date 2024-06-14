package org.example.kafkatest2.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class SimpleProducer {
    @Value("${kafka.bootstrap-server}")
    String BOOTSTRAP_SERVERS_CONFIG;
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    static boolean readyFlag = false;

    private static KafkaProducer<String, String> producer = null;
    public SimpleProducer() {

    }
    public boolean readyProducer(){
        try{
            Properties configs = new Properties();
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());


            producer = new KafkaProducer<>(configs);
            readyFlag = true;
            return true;
        }catch(Exception e){
            logger.error("Error while creating producer: {}", e.getMessage());
            return false;
        }

    }

    public void sendData(String topicName,String key, String messageValue) {
        if(!readyFlag) {
            readyProducer();
        }

        if(messageValue == null || messageValue.isEmpty()) {
            logger.error("messageValue is null or empty");
            return;
        }else{
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key,messageValue);
            RecordMetadata metaData = null;
            logger.info("producer : "+ producer);
            logger.info(" readyFlag: "+ readyFlag);
            producer.send(record, new ProducerCallback());
        }
    }

    public void autoSendData(int requestCount, String topicName,String key, String messageValue) {
        if(!readyFlag) {
            readyProducer();
        }

        if(messageValue == null || messageValue.isEmpty()) {
            logger.error("messageValue is null or empty");
            return;
        }else{
            for(int i = 0 ; i<requestCount; i++){
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key + i,messageValue + i);
                logger.info("producer : "+ producer);
                logger.info(" readyFlag: "+ readyFlag);
                // 1초 주기로 producer에 데이터를 전송한다.
                try {
                    Thread.sleep(10);
                    producer.send(record, new ProducerCallback());

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }




//                producer.send(record, new ProducerCallback());
            }
        }
    }

}
