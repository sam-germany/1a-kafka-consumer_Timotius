package com.course.kafkaconsumer.consumer;

import com.course.kafkaconsumer.entity.CarLocation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.IOException;

//@Service
public class CarLocationConsumer {
    private static final Logger log = LoggerFactory.getLogger(CarLocation.class);

    private ObjectMapper objectMapper22 = new ObjectMapper();

    @KafkaListener(topics = "t_location", groupId = "cg-all-location")
    public void listenAll(String message) throws IOException {
        var carLocation = objectMapper22.readValue(message, CarLocation.class);
        log.info("listenAl {} ", carLocation);
    }

    @KafkaListener(topics = "t_location", groupId = "cg-far-location", containerFactory = "farLocationContainerFactory22")
    public void listenFar(String message) throws IOException {
        var carLocation = objectMapper22.readValue(message, CarLocation.class);
        log.info("listenFar {} ", carLocation);
    }
}
