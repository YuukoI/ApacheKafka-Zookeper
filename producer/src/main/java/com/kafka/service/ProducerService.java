package com.kafka.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class ProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message){
        kafkaTemplate.send("topic", message).whenComplete((res, e) -> {
            if(e != null){
                log.error("Error {}",e.getMessage());
            }
            log.info("Message sent successfully {}", res.getProducerRecord().value());
            log.info("Partition {}, Offset {}", res.getRecordMetadata().partition(), res.getRecordMetadata().offset());
        });
    }

}
