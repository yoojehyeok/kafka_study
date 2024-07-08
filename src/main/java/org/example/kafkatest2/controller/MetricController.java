package org.example.kafkatest2.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.example.kafkatest2.kafka.pipeline.MetricStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RestController
@RequiredArgsConstructor
@RequestMapping("/metric")
public class MetricController {

//    private final static Logger logger = LoggerFactory.getLogger(MetricController.class);

    @RequestMapping("/infoStart")
    public String metricInfo() {
        MetricStreams metricStreams = new MetricStreams();
        metricStreams.metricStreamsStart();

        return "started";
    }



}
