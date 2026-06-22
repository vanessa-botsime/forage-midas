package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incetive;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.entity.UserRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    @Autowired
    private DatabaseConduit databaseConduit;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private RestTemplate restTemplate;

    private static final String INCENTIVE_URL = "http://localhost:8080/incentive";

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        System.out.println("Received transaction: " + transaction);

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) return;

        Incetive incetive = restTemplate.postForObject(
            INCENTIVE_URL,
            transaction,
            Incetive.class
        );

        float incentiveAmount = (float) ((incetive != null) ? incetive.getAmount() : 0);

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        databaseConduit.save(sender);
        databaseConduit.save(recipient);
    }
}