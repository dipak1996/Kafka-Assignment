package com.example.kafka.assignment.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;

@Service
public class KafkaOffsetConsumerService {

    @Autowired
    private KafkaConsumer<String, String> kafkaConsumer;

    public void consumeMessages(String topic, long startOffset, long endOffset) {
        TopicPartition partition = new TopicPartition(topic, 3);
        kafkaConsumer.assign(Collections.singletonList(partition));
        kafkaConsumer.seek(partition, startOffset);
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.offset() > endOffset) {
                        kafkaConsumer.commitSync();
                        return;
                    }
                    // Manually commit the offset after processing each message
                    kafkaConsumer.commitSync(Collections.singletonMap(
                            partition, new OffsetAndMetadata(record.offset() + 1)));
                }

            }
        } catch (Exception e) {
            System.err.println("Error while consuming messages: " + e.getMessage());
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}


