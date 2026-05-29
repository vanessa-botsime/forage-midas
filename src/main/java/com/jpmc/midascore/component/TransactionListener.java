package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        System.out.println("Received transaction: " + transaction);
    }
}
// The TransactionListener class acts as an automated network antenna.By adding @Component, Spring Boot continuously monitors this class.By adding @KafkaListener(topics = "..."), Spring opens a live connection to that topic. Every time data enters the stream, Spring intercepts it, reconstructs it back into a Java Transaction object, and hands it directly to your listen() method.