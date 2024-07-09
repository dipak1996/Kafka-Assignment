package com.example.kafka.assignment.controller;

import com.example.kafka.assignment.consumer.KafkaOffsetConsumerService;
import com.example.kafka.assignment.producer.EventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EventController {

    private final EventProducer messageProducer;

    public EventController(EventProducer messageProducer) {
        this.messageProducer = messageProducer;
    }

    @Autowired
    private KafkaOffsetConsumerService kafkaOffsetConsumerService;

    @PostMapping("/setOffsets")
    public String setOffsets(@RequestParam String topic, @RequestParam long startOffset, @RequestParam long endOffset) {
        kafkaOffsetConsumerService.consumeMessages(topic, startOffset, endOffset);
        return "Messages consumed from offset " + startOffset + " to " + endOffset;
    }

    @PostMapping("/send")
    public String sendMessage(@RequestParam("message") String message) {
        messageProducer.sendMessage(message);
        return "Message sent: " + message;
    }

}
