package com.course.kafkaconsumer.error.handler;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

//ConsumerAwareListenerErrorHandler    <-- this we need when we want to create a error handler for a single class
// as we have defined also a Global Error handler but here we are defining extra class level error handler
// so in this case class level have priority then global error handler, but if we again throw a error from this
// overridden class then it will be handled by Global error handler
@Service(value = "myFoodOrderErrorHandler")
public class FoodOrderErrorHandler implements ConsumerAwareListenerErrorHandler {
     private static final Logger log = LoggerFactory.getLogger(FoodOrderErrorHandler.class);

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
      log.warn("Food order error. Pretending to send to elasticsearch : {}, because: {} ", message.getPayload(), exception.getMessage());

      if(exception.getCause() instanceof RuntimeException) {
          throw exception;
      }    // here we are again rethrowing the exception. now this exception will be handeld by Global error handler

        return null;
    }


}
