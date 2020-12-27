package com.course.kafkaconsumer.consumer;

import com.course.kafkaconsumer.entity.SimpleNumber;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class SimpleNumberConsumer {
    private static final Logger log = LoggerFactory.getLogger(SimpleNumberConsumer.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "t_simple_number")
    public void consumer22(String message) throws IOException {
        var simpleNumber22 = objectMapper.readValue(message, SimpleNumber.class);

        if(simpleNumber22.getNumber() %2 != 0) {
            throw new IllegalArgumentException("Odd number");
        }
        log.info("Valid number :  {}", simpleNumber22);
    }


}
