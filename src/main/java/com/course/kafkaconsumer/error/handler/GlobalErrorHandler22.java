package com.course.kafkaconsumer.error.handler;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;

// ConsumerAwareErrorHandler  <-- this we need when we want to create a Global error handler for this whole application
//Note: here we define global error handler so basically we are overriding the spring default error handler with our
// own defined this class GlobalErrorHander22   so for overriding we need to set this class in the @Configuration
// this we in "kafkaConfig"  ConcurrentKafkaListenerContainerFactory    <-- here we have done this see the method
// farLocationContainerFactory33() { }
public class GlobalErrorHandler22 implements ConsumerAwareErrorHandler {

   private static final Logger log = LoggerFactory.getLogger(ConsumerAwareErrorHandler.class);

   @Override
   public void handle(Exception thrownException, ConsumerRecord<?, ?> data, Consumer<?, ?> consumer) {

       log.warn("Global error handler for message  {} ", data.value().toString());
    }
}
