package com.course.kafkaconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

//@Service
public class RebalanceConsumer {
    private static final Logger logger = LoggerFactory.getLogger(RebalanceConsumer.class);

    @KafkaListener(topics = "t_rebalance", concurrency = "3")
    public void consume(ConsumerRecord<String, String > message) {
        logger.info("Partition : {} , offset {} .  Message {}",message.partition(), message.offset(), message.value());

    }


}
