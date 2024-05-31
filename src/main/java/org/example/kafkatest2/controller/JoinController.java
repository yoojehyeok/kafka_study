package org.example.kafkatest2.controller;

import lombok.RequiredArgsConstructor;
import org.example.kafkatest2.kafka.join.JoinStream;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/join")
@RequiredArgsConstructor
public class JoinController {

    @PostMapping("/start")
    public String joinData(@RequestParam String topicName1, @RequestParam String topicName2){

        JoinStream joinStream = new JoinStream();
//        joinStream.startJoin(topicName1, topicName2);

        return "joinData start";
    }


}
