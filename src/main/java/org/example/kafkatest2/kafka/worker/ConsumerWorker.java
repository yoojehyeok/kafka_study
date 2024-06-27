package org.example.kafkatest2.kafka.worker;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerWorker implements Runnable{
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>();
    private static Map<Integer, Long> currentFileOffset = new ConcurrentHashMap<>();
    private final static int FLUSH_RECORD_COUNT = 10;
    public ConsumerWorker(Properties prop, String topic, int number)
    {
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(threadName);
        consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Arrays.asList(topic));
        try{
            while(true){
                logger.info("running thread : " + threadName);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    addHdfsBuffer(record);
                    logger.info("thread_inner: {} , recordValue: {}",Thread.currentThread().getName(),record.value());
                });
                saveBufferToHdfsFile(consumer.assignment());
            }
        }catch(WakeupException e){
            logger.warn(e.getMessage());
        }catch(Exception e){
            logger.error(e.getMessage(), e);
        }finally{
            consumer.close();
        }
    }
    public void addHdfsBuffer(ConsumerRecord<String, String> record){
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);

        if(buffer.size() == 1){
            currentFileOffset.put(record.partition(), record.offset());
        }
    }

    private void saveRemainBufferToHdfsFile(){
        bufferString.forEach((partitionNo, v) -> this.save(partitionNo));
    }

    private void saveBufferToHdfsFile(Set<TopicPartition> partitions) {
        partitions.forEach(p -> checkFlushCount(p.partition()));
    }

    private void checkFlushCount(int partitionNo) {
        if(bufferString.get(partitionNo) != null){
            if(bufferString.get(partitionNo).size() >= FLUSH_RECORD_COUNT -1){
                save(partitionNo);
            }
        }
    }

    private void save(int partitionNo){
        if(bufferString.get(partitionNo).size() >0){
            try{
                String fileName = "/data/color-" + partitionNo + "_" +
                        currentFileOffset.get(partitionNo) +".log";
                Configuration configuration = new Configuration();
                configuration.set("'fs.defaultFS", "hdfs://125.181.184.135:2117");
                FileSystem hdfsFileSystem = FileSystem.get(configuration);
                FSDataOutputStream fileOutputStream = hdfsFileSystem. create(new Path(fileName));
                fileOutputStream.writeBytes(StringUtils.join(bufferString.get(partitionNo), "\n"));
                fileOutputStream.close();
                bufferString.put(partitionNo, new ArrayList<>());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public void stopAndWakeup () {
        logger.info ("stopAndWakeup");
        consumer.wakeup() ;
        saveRemainBufferToHdfsFile();
    }


}
