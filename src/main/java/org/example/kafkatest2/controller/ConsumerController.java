package org.example.kafkatest2.controller;

import lombok.RequiredArgsConstructor;
import org.example.kafkatest2.kafka.consumer.SimpleConsumer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RequestMapping("/consumer")
@RestController
@RequiredArgsConstructor
public class ConsumerController {

    private final SimpleConsumer simpleConsumer;

    @GetMapping("/start")
    public String consumeData(@RequestParam String topicName){
        simpleConsumer.consumeStart(topicName);
        return "consumeData";
    }

    @PostMapping("/stop")
    public String stopConsumeData() {
        simpleConsumer.consumeStop();
        return "stopConsumeData";
    }
}