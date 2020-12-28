package com.course.kafkaconsumer.config;

import com.course.kafkaconsumer.entity.CarLocation;
import com.course.kafkaconsumer.error.handler.GlobalErrorHandler22;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.FixedBackOff;

import java.io.IOException;

@Configuration
public class KafkaConfig {

    @Autowired                 // KafkaProperties is a predefined class, we can define any configuration for kafka
    private KafkaProperties kafkaProperties22;// it is same as we can define any property in the application.yml
    // file or we can define here in the @Configuration class, if we have defined any property in the .yml file
    // and here we can also override that property by re-defining it.

//video 44-45
    @Bean     //to understand this ConsumerFactory it is same as ProducerFactory, i wrote every step explanition in the
    public ConsumerFactory<Object,Object> consumerFactory22() {                       // Producer application
          var properties22 = kafkaProperties22.buildConsumerProperties();
          properties22.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "120000"); // to set the time for auto-refreshing
                   //by the Kafka internally, like it will go  for the producer and check for the  configurations if
                                                                        // any this is updated in the configuration
         return new DefaultKafkaConsumerFactory<Object,Object>(properties22);
    }
/*Note: Spring has created many predefined configuration object for us, but we can also override those predefined
configure object with our own configuration,for this we need to do these steps

Step 1) first create that object as we are creating  "ProducerFactory<String,String>"

Step 2) for configuring it we need to pass properties, to pass these properties we need "KafkaProperties"
        as we are passing above  "properties22.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "180000");"

Step3) at the end we need to create a "new KafkaTemplate<>"  object  with @Bean, so at run time these
       new configuration will be taken by the Spring framework
 */


// video 48
    @Bean(name ="farLocationContainerFactory22")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> farLocationContainerFactory22(
                                                 ConcurrentKafkaListenerContainerFactoryConfigurer configurer22) {

        var factory22 = new ConcurrentKafkaListenerContainerFactory<Object,Object>();
        configurer22.configure(factory22, consumerFactory22());  // .configure() method need as argument ConsumerFactory()
                                        // with new properties that we want to define with this new confiugration
        // as above explained that we can define our own configuration by overriding predefined ConsumerFactory object
        factory22.setRecordFilterStrategy(new RecordFilterStrategy<Object, Object>() {

            ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public boolean filter(ConsumerRecord<Object, Object> consumerRecord22) {
               try {
                    var carLocation = objectMapper.readValue(consumerRecord22.value().toString(), CarLocation.class);
                    return carLocation.getDistance() <= 100;
               }catch (IOException e) {
                  return false;
               }
            }
        });
        return factory22;
    }
// vidoe 49
    @Bean(value = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory33(
                                            ConcurrentKafkaListenerContainerFactoryConfigurer configurer22) {

        var factory22 = new ConcurrentKafkaListenerContainerFactory<Object,Object>();
        configurer22.configure(factory22, consumerFactory22());

        factory22.setErrorHandler(new GlobalErrorHandler22());
        return factory22;
    }

    private RetryTemplate createRetryTemplate22(){
        var retryTemplate22 = new RetryTemplate();
        var retryPolicy  = new SimpleRetryPolicy(3);  // we are defining maximum 3 time retry
        retryTemplate22.setRetryPolicy(retryPolicy);

        var backOffPolicy22 = new FixedBackOffPolicy();
        backOffPolicy22.setBackOffPeriod(10000);       //<-- here we are setting that kafka should take 10sec
        retryTemplate22.setBackOffPolicy(backOffPolicy22);                        // gap inbetween every retry

        return retryTemplate22;     //Note: How retry works as we have defined 3 times above
    }                           // First attempt 0sec (zero) --   Second attempt 10sec  -- Third attempt 20sec
                                // these attempts will not move like 10-20-30
// video 50
    @Bean(value ="imageRetryContainerFactory22")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> imageRetryContainerFactory22(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer22) {

        var factory22 = new ConcurrentKafkaListenerContainerFactory<Object,Object>();
        configurer22.configure(factory22, consumerFactory22());

        factory22.setErrorHandler(new GlobalErrorHandler22());
        factory22.setRetryTemplate(createRetryTemplate22());

        return factory22;
    }

//video 53
     @Bean(value = "invoiceDeadLetterTopicFactory")
     public ConcurrentKafkaListenerContainerFactory<Object, Object> invoiceDeadLetterTopicFactory(
                   ConcurrentKafkaListenerContainerFactoryConfigurer configurer, KafkaOperations <Object, Object> KafkaOperations) {

        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory22());

        var recoverer = new DeadLetterPublishingRecoverer(KafkaOperations,
                                         (record, ex) -> new TopicPartition("t_invoice_dlt", record.partition()));
        // as in the begining we have created 2 topics "t_invoice" and "t_invoice_dlt" with same number of partitions,
       //  assume this message come is stored in the "t_invoice" topic in 0th partition then here we are also trying
       // to send this message to the "t_invoice_dlt" topic 0th  partition nr

        // 5 retry, 10 second interval for each retry
        var errorHandler = new SeekToCurrentErrorHandler(recoverer, new FixedBackOff(10000, 5));

        factory.setErrorHandler(errorHandler);

    return factory;
}






}




