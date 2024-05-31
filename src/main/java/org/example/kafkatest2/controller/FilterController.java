package org.example.kafkatest2.controller;

import lombok.RequiredArgsConstructor;
import org.example.kafkatest2.kafka.filter.SimpleFilter;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/filter")
public class FilterController {

    private final SimpleFilter simpleFilter;

    @PostMapping("/start")
    public String filterData(){
        simpleFilter.filterData();
        return "filterData start";
    }

}
