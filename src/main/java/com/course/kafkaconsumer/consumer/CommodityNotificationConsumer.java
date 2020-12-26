package com.course.kafkaconsumer.consumer;

import com.course.kafkaconsumer.entity.Commodity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class CommodityNotificationConsumer {
    private static final Logger log = LoggerFactory.getLogger(CommodityNotificationConsumer.class);

    ObjectMapper objectMapper22 = new ObjectMapper();

    @KafkaListener(topics = "t_commodity", groupId = "cg-notification")
    public void consumer(String message) throws IOException, InterruptedException {
        var commodity = objectMapper22.readValue(message, Commodity.class);

        Thread.sleep(ThreadLocalRandom.current().nextLong(500, 1000));

        log.info("Notification logic for {}---" , commodity);
    }



}
