package org.example.kafkatest2.controller;

import lombok.RequiredArgsConstructor;
import org.example.kafkatest2.kafka.producer.SimpleProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/producer")
@RequiredArgsConstructor
public class ProducerController {

    private final SimpleProducer simpleProducer;

    @PostMapping("/send")
    public String sendData(@RequestParam String messageValue){
        simpleProducer.sendData(messageValue);
        return "sendData";
    }
}
