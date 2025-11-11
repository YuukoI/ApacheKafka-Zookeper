package com.kafka.listener;

import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class ConsumerListener {

    @KafkaListener(groupId = "group-1", topicPartitions = @TopicPartition(topic = "topic", partitions = {"0"}), containerFactory = "validMessageContainerFactory")
    public void listener1(String message){
        log.info("Consumer Listener {}", message);
        log.info("LISTENER 1 --- Receiveng Message: {}", message);
    }

    @KafkaListener(groupId = "group-1", topicPartitions = @TopicPartition(topic = "topic", partitions = {"1"}), containerFactory = "validMessageContainerFactory")
    public void listener2(String message){
        log.info("Consumer Listener {}", message);
        log.info("LISTENER 2 --- Receiveng Message: {}", message);
    }

    @KafkaListener(groupId = "group-2", topics = "topic", containerFactory = "validMessageContainerFactory")
    public void listener3(String message){
        log.info("Consumer Listener {}", message);
        log.info("LISTENER 3 --- Receiveng Message: {}", message);
    }

}
