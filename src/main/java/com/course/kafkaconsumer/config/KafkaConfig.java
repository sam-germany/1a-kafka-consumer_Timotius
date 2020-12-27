package com.course.kafkaconsumer.config;

import com.course.kafkaconsumer.entity.CarLocation;
import com.course.kafkaconsumer.error.handler.GlobalErrorHandler22;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

import java.io.IOException;

@Configuration
public class KafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties22;

             // for overriding Consumer configuration we need to write KafkaTemplate, this KafkaTemplate we need only
    @Bean                                                                      // for overriding Producer configuration
    public ConsumerFactory<Object,Object> consumerFactory22() {
          var properties22 = kafkaProperties22.buildConsumerProperties();
          properties22.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "120000");

         return new DefaultKafkaConsumerFactory<Object,Object>(properties22);
    }
// video 48
    @Bean(name ="farLocationContainerFactory22")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> farLocationContainerFactory22(
                                                 ConcurrentKafkaListenerContainerFactoryConfigurer configurer22) {

        var factory22 = new ConcurrentKafkaListenerContainerFactory<Object,Object>();
        configurer22.configure(factory22, consumerFactory22());

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
    public ConcurrentKafkaListenerContainerFactory<Object, Object> farLocationContainerFactory33(
                                            ConcurrentKafkaListenerContainerFactoryConfigurer configurer22) {

        var factory22 = new ConcurrentKafkaListenerContainerFactory<Object,Object>();
        configurer22.configure(factory22, consumerFactory22());

        factory22.setErrorHandler(new GlobalErrorHandler22());
        return factory22;
    }

}




