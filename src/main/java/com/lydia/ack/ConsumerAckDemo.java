package com.lydia.ack;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * 让消费者Consumer手工确认消费成功Ack,需要spring.kafka.listener.ack-mode=manual，引入ackApplication.properties
 */
@SpringBootApplication
@PropertySource(value={"classpath:ackApplication.properties"})
@RestController
@Slf4j
public class ConsumerAckDemo {

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    public static void main(String[] args) {
        SpringApplication.run(ConsumerAckDemo.class, args);
    }

    /**
     * 单-生产者
     * @param input
     */
    @GetMapping("/send/{input}")
    public void receiveAndSendToKafka(@PathVariable String input) {
        //生产者，把收到的消息用KafkaTemplate发出去
        this.kafkaTemplate.send("topic_ack_test", input);
    }

    /**
     * 消费者,当收到hulu这个消息时发送ack确认消费成功
     */
    @KafkaListener(id = "ack_group", topics = "topic_ack_test")
    public String ackConsumer(String input, Acknowledgment ack) {
        log.info("消费者收到消息: {}", input);
        if ("hulu".equals(input)) {
            //ack.acknowledge();代表发送ack消息给服务器
            ack.acknowledge();
            return "successful";
        }
        return "fail";
    }

}
