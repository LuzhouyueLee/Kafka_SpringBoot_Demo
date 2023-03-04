package com.lydia.firstDemo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * 创建单-生产者，双消费者Demo
 * @author: Lydia Lee
 * @date: 2023/3/03
 */
@SpringBootApplication
@RestController
@Slf4j
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    /**
     * 单-生产者
     * @param input
     */
    @GetMapping("/send/{input}")
    public void receiveAndSendToKafka(@PathVariable String input) {
        //生产者，把收到的消息用KafkaTemplate发出去
        //
        this.kafkaTemplate.send("topic_hello", input);
    }

    /**
     * 消费者1
     * @param input
     */
    @KafkaListener(id = "rest1", topics = "topic_hello")
    public void listen1(String input) {
        log.info("Kafka Consumer1 gets value: {}", input);
    }

    /**
     * 消费者2
     * @param input
     */
    @KafkaListener(id = "rest2", topics = "topic_hello")
    public void listen2(String input) {
        log.info("Kafka Consumer2 gets value: {}", input);
    }
}