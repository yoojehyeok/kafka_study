package org.example.kafkatest2.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@RestController
@RequiredArgsConstructor
@RequestMapping("/admin")
public class AdminController {

    private final static Logger logger = LoggerFactory.getLogger(AdminController.class);

    @Value("${kafka.bootstrap-server}")
    String bootstrapServer;
    @RequestMapping("/info")
    public void brokerInfo() {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        AdminClient admin = AdminClient.create(configs);

        logger.info("Broker Info: {}", admin.describeCluster());
        try {
            for (Node node : admin.describeCluster().nodes().get()) {
                logger.info("Node Info: {}", node);
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
                DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(configResource));
                describeConfigs.all().get().forEach((broker, config) -> {
                    config.entries().forEach((configEntry -> {
                        logger.info(configEntry.name() + " : " + configEntry.value());
                    }));
                });
            }

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }



}
