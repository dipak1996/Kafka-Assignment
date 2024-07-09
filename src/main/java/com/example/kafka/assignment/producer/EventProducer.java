package com.example.kafka.assignment.producer;

import com.example.kafka.assignment.model.UserDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventProducer {
    @Value("${topic.name}")
    private String topic;

    @Value("${json.message.topic.name}")
    private String users;


    private final KafkaTemplate<String, String> kafkaTemplate;


    private final KafkaTemplate<String, UserDTO> kafkaJsonTemplate;

    public EventProducer(KafkaTemplate<String, String> kafkaTemplate, KafkaTemplate<String, UserDTO> kafkaJsonTemplate ) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaJsonTemplate = kafkaJsonTemplate;
    }

    public void sendMessage(String message) {

        kafkaTemplate.send(topic, message);
        log.info("Message : {} sent to topic : {}",message,topic);
    }

    public void sendJsonMessage(UserDTO user)
    {
        kafkaJsonTemplate.send(users,user);
        log.info("Message : {} sent to topic : {}",user.toString(),topic);
    }
}
