package com.course.kafkaconsumer.consumer;

import com.course.kafkaconsumer.entity.Invoice;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.IOException;

//@Service
public class InvoiceConsumer {
    private static Logger log = LoggerFactory.getLogger(InvoiceConsumer.class);

    private ObjectMapper objectMapper22 = new ObjectMapper();

    @KafkaListener(topics = "t_invoice", containerFactory = "invoiceDeadLetterContainerFactory")
    public void consume22(String message) throws IOException {
        var invoice = objectMapper22.readValue(message, Invoice.class);

        if(invoice.getAmount() < 1) {
            throw  new IllegalArgumentException("Invoice amount : "+ invoice.getAmount());
        }
        log.info("Processing invoice {} ", invoice);
    }

}
