package org.example.kafkatest2.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner{
    @Override
    public void configure(Map<String, ?> map) {}


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        if(keyBytes == null) {
            throw new InvalidRecordException("key cannot be null for partitioning");
        }

        if(valueBytes == null) {
            throw new InvalidRecordException("value cannot be null for partitioning");
        }

        if(key.toString().equals("pangYo")) {
            return 0;
        }

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;

    }

    @Override
    public void close() {

    }
}
