package com.course.kafkaconsumer.consumer;

import com.course.kafkaconsumer.entity.Image;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.IOException;

//@Service
public class ImageConsumer {
   private static final Logger log = LoggerFactory.getLogger(ImageConsumer.class);

   private ObjectMapper objectMapper22 = new ObjectMapper();

   @KafkaListener(topics = "t_image", containerFactory = "imageRetryContainerFactory22")
   public void consumer(String message) throws IOException {
       var image = objectMapper22.readValue(message, Image.class);

       if(image.getType().equals("svg")) {
           throw new RuntimeException("Simulate failed Api call");
       }

       log.info("Processing image:  {} ", image);
   }
}
/*
Note: main point to understand is as we are throwing an Exception on a message and we set in kafkaConfig class
retry option, so when we are throwing a exception here then kafka again retry to fetch the message as we define
3 times so total it try to fetch 3 time including first call and here we are everytime throwing error so
after third try as it is not successfull then the execute this Exception and throw this Exception, But it does
not stop execution it moves forward and fetch the rest messages
Main point is it will not thow Exception in the firs attempt, it will retry till third attempt and after
that still not successfull then it throw the exception

 */