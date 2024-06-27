package org.example.kafkatest2.kafka.pipeline;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.*;

import java.util.Map;

public class ElasticSearchSinkConnectorConfig extends AbstractConfig {
        public static final String ES_CLUSTER_HOST = "125.181.184.135";
        private static final String ES_CLUSTER_HOST_DEFAULT_VALUE = "125.181.184.135";
        private static final String ES_CLUSTER_HOST_DOC = "125.181.184.135";
        public static final String ES_CLUSTER_PORT = "2118";
        private static final String ES_CLUSTER_PORT_DEFAULT_VALUE = "2118";
        private static final String ES_CLUSTER_PORT_DOC = "2118";
        public static final String ES_INDEX = "es.index";
        private static final String ES_INDEX_DEFAULT_VALUE = "kafka-connector-index";
        private static final String ES_INDEX_DOC = "엘라스틱서치인덱스를입력";
        public static ConfigDef CONFIG = new ConfigDef().define(ES_CLUSTER_HOST, Type.STRING, ES_CLUSTER_HOST_DEFAULT_VALUE, ConfigDef.Importance.HIGH, ES_CLUSTER_HOST_DOC)
                .define (ES_CLUSTER_PORT, Type.INT, ES_CLUSTER_PORT_DEFAULT_VALUE, Importance. HIGH, ES_CLUSTER_PORT_DOC)
                .define(ES_INDEX, Type. STRING, ES_INDEX_DEFAULT_VALUE, Importance.HIGH, ES_INDEX_DOC) ;
        public ElasticSearchSinkConnectorConfig(Map<String, String> props) {
            super (CONFIG, props);
        }
}
