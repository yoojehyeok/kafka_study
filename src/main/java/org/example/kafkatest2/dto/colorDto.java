package org.example.kafkatest2.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@ToString
public class colorDto {
    private String timestamp;
    private String userAgent;
    private String colorName;
    private String userName;



}
