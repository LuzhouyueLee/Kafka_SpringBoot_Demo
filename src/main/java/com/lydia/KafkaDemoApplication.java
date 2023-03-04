package com.lydia;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author: Lydia Lee
 * @date: 2023/3/03
 */
@SpringBootApplication
@RestController
public class KafkaDemoApplication {

    private final Logger logger = LoggerFactory.getLogger(KafkaDemoApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

    @Autowired
    private KafkaTemplate<Object, Object> template;

    @GetMapping("/send/{input}")
    public void sendFoo(@PathVariable String input) {
        this.template.send("topic_hello", input);
    }

    @KafkaListener(id = "rest1", topics = "topic_hello")
    public void listen1(String input) {
        logger.info("Kafka Lister1 gets value: {}", input);
    }

    @KafkaListener(id = "rest2", topics = "topic_hello")
    public void listen2(String input) {
        logger.info("Kafka Lister2 gets value: {}", input);
    }
}