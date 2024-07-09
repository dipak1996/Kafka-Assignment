package com.example.kafka.assignment.consumer;


import com.example.kafka.assignment.model.UserDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventConsumer {


    @KafkaListener(topics = "users", groupId = "my-group-id")
    public void listen(String message) {
        log.info("Received message: " + message);
    }


    @KafkaListener(topics = {"${json.message.topic.name}"}, containerFactory = "kafkaListenerJsonFactory", groupId = "group_id")
    public void consumeSuperHero(UserDTO user) {
        log.info("**** -> Consumed User :: {}", user);
    }

}
